<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-88: New Schema/DataType/Expression Abstractions

## Proposers

- @cshuo
- @danny0405
- @voonhous

## Approvers
- ..

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-8966

Restructured in 2026 to build on `HoodieSchema` from [RFC-99](../rfc-99/rfc-99.md), which closes with a reduced scope
([#20488](https://github.com/apache/hudi/pull/20488)) and hands its remaining types, translation layers and Avro-free
internals to this RFC.

## Abstract

Hudi represents a schema in four places: the engine (Spark, Flink, Trino, Hive), `HoodieSchema`, the physical file
format (Parquet, Lance, ORC, HFile, Avro) and the catalog (HMS, Glue, BigQuery). Today every read, write, filter and sync
path carries its own conversions between them, so the same logical type behaves differently depending on which engine
and which path touched it, and every engine x format pair needs its own reader and writer code.

This RFC splits the problem in two:

* **Type plane.** `HoodieSchema` (RFC-99) is the hub. One mapper per boundary converts to and from it, and every mapper
  declares, per type, whether it maps natively, lossily or not at all. Engines, formats and catalogs never map to each
  other directly.
* **Data plane.** The operations that move rows and predicates across those boundaries: write, read, filter / prune and
  catalog sync. Formats read and write a single Hudi row type that engines wrap, and engine filters are translated into
  Hudi `Expression` once and rendered out to readers and catalogs.

The data plane uses the type plane; the type plane depends on nothing in the data plane, so it can land first.

![Overview: type plane and data plane](overview.svg)

## Background

### Schema conversions are duplicated per boundary

`HoodieSchema` is Hudi's schema API (RFC-99), but the code that maps it to other systems is spread across engines and
formats, and the copies disagree:

* **Parquet `MessageType` is produced three ways**: by Spark's row writer from `StructType`
  (`HoodieRowParquetWriteSupport`), by Flink's `ParquetSchemaConverter`, and by the parquet-avro path through Hudi's copy
  of the Avro converter. Lance's Arrow schema is produced two ways (Spark from `StructType` with a BLOB patch, Flink from
  `HoodieSchema`).
* **Three Hive mappings**: `HiveTypeUtils` (hadoop-mr), `HiveSchemaUtil` (Hive sync) and the Flink catalog's
  `HiveSchemaUtils`. Trino builds its column types from the Hive type strings in the metastore rather than from
  `HoodieSchema`.
* **No loss is declared anywhere.** Examples, all on master:
  * Flink TINYINT / SMALLINT and Spark `ByteType` / `ShortType` map to `HoodieSchemaType.INT`. Flink's Parquet writer
    derives its schema from `HoodieSchema`, so it writes plain INT32; Spark's row writer derives it from `StructType`, so
    it annotates `ShortType` as `INT(16)` and throws on `ByteType`. The width is lost and the physical layout depends on
    the writer. RFC-99's mapping table says SMALLINT is `INT32 + INTEGER(16)`.
  * UUID is declared `binary` by Hive sync and `STRING` by BigQuery sync.
  * BigQuery sync maps every DECIMAL to `NUMERIC`, which holds at most scale 9.
  * Hive sync stores VECTOR as `binary`.

![Today: schema conversions per engine](type-plane-today.svg)

### Rows take detours through Avro, and readers and writers exist per engine x format

On write Hudi receives an engine row (`InternalRow`, `RowData`, `IndexedRecord`). Each engine x format pair is either a
dedicated class, a detour through Avro, or missing:

| Engine | Parquet | ORC | Lance | HFile | Avro log block |
|---|---|---|---|---|---|
| Spark | `HoodieSparkParquetWriter` | via Avro (AVRO record type only) | `HoodieSparkLanceWriter` | via `toIndexedRecord` (AVRO record type only) | row converted to Avro (`getAvroBytes`) |
| Flink | `HoodieRowDataParquetWriter` | not supported | `HoodieRowDataLanceWriter` | via Avro | row converted to Avro (`getAvroBytes`) |
| Avro / Java | `HoodieAvroParquetWriter` | `HoodieAvroOrcWriter` | not supported | `HoodieAvroHFileWriter` | `GenericDatumWriter` |

Factories pick writers by record type x file format, so every gap is a runtime exception. On the read side, Flink's
vectorized Parquet reader is copied once per Flink version, Trino merges file groups in Avro (`HudiAvroSerializer`), and
Hive's Avro-based readers convert each record to `ArrayWritable`. Per-record Avro conversion therefore remains on hot
paths even after RFC-46 and RFC-84: Spark's default AVRO record type (rows become Avro before any writer, owned by RFC-115
and the merger API), Avro and HFile log blocks, Trino and Hive reads.

Avro data blocks are the default log block only for the inline log format (write version below 10). From version 10,
the default, log files are native base-format files written through the same factory as base files.

![Today: file writers and readers per engine](writer-reader-today.svg)

### Expressions are translated per engine

Hudi's `Expression` (in `hudi-common`) is used by Spark partition pruning, Hive / Glue sync filters and metadata-table key
lookups only. Everything else runs on each engine's own expression model:

* Column-stats data skipping is implemented three times: Spark rewrites Catalyst into min / max Catalyst and evaluates
  it as a DataFrame (`DataSkippingUtils`), Flink builds `ExpressionEvaluators` over `RowData`, Trino builds `Domain`s.
  Order-breaking casts and overflow bugs are fixed per engine.
* Index coverage differs: the expression index and metadata-table bloom filters are Spark only; Hive has no data
  skipping.
* Literal typing differs: `SparkFilterHelper` maps Spark types to `InternalSchema` types (a `LocalDate` literal is typed
  as a timestamp holding epoch days), Flink switches on `LogicalTypeRoot`, Trino builds `Domain`s.

![Today: expression and filter pushdown per engine](expression-today.svg)

## Goals

* One mapping per boundary between `HoodieSchema` and each engine, format and catalog, with declared loss and shared
  tests, so a type behaves the same everywhere.
* One reader and one writer per file format, usable by every engine without per-record Avro conversion.
* One translation of engine filters into Hudi `Expression`, so partition pruning, data skipping and catalog filters
  behave the same for every engine.

## Non-goals

* Rewriting existing files. Avro / HFile log blocks keep their encoding. Where today's write paths differ (the
  `INT(16)` label, list structure, `bytes`-backed decimals), the converged mapper only emits layouts that a current
  writer already emits and keeps today's knobs as `PhysicalOptions` with today's defaults, so supported readers already
  read every file it writes. A layout no current writer produces waits for a table-version bump.
* Replacing the persisted schema. It stays Avro JSON plus Hudi logical-type properties, read and written through
  `HoodieSchema.parse` / `toString` (RFC-99); the first version of this RFC proposed a new persisted format.
* Schema evolution semantics, field ids and retiring `InternalSchema`, owned by
  [RFC-104](https://github.com/apache/hudi/pull/18661).
* The record merger API (RFC-101). Payload-based and custom record merging still convert records to Avro; removing that
  belongs to payload deprecation (RFC-97) and the merger API.
* Replacing engine vectorized readers.

## Design

| Piece | Owner |
|---|---|
| `HoodieSchema`, the type set and RFC-99's interoperability table (the spec) | RFC-99 |
| Type plane: mappers, `TypeSupport`, catalog renderers, shared tests; the types RFC-99 left open; Avro-free internals | this RFC |
| Data plane: reader / writer, `HoodieRecord` / `KeyGenerator`, expressions | this RFC |
| Field ids, schema evolution, retiring `InternalSchema` | RFC-104 |

### 1. Type plane: mapping `HoodieSchema` to every boundary

![Proposed: type plane](type-plane-proposal.svg)

Every mapper converts between `HoodieSchema` and exactly one other type system. Rules:

1. Engines, formats and catalogs never map to each other directly; everything goes through `HoodieSchema`.
2. Every mapper declares, per `HoodieSchemaType` and options, whether it maps `NATIVE`, `LOSSY` (with a reason) or
   `UNSUPPORTED`. Writing an unsupported type fails fast.
3. `fromX(toX(s))` equals `s` for every `NATIVE` type. A hint (the table schema) resolves types a system cannot express,
   such as BLOB or VECTOR carried as a struct or array.
4. Every mapper passes the same shared test kit, and RFC-99's interoperability table is its expected output.

```java
public final class TypeSupport {
  public enum Level { NATIVE, LOSSY, UNSUPPORTED }
  Level level();
  String reason();                  // required for LOSSY, e.g. "VECTOR stored as Hive binary"
}

/** Engines and catalogs that have their own type system: Spark, Flink, Trino, Hive, BigQuery. */
public interface TypeSystemMapper<T> {
  String typeSystem();
  T toTypeSystem(HoodieSchema schema, MappingOptions opts);
  HoodieSchema fromTypeSystem(T type, Option<HoodieSchema> hint);
  TypeSupport support(HoodieSchemaType type, MappingOptions opts);
}

/** File formats: the only place a physical schema is decided. */
public interface FormatSchemaMapper<P> {
  HoodieFileFormat format();
  P toPhysicalSchema(HoodieSchema schema, PhysicalOptions opts);   // legacy list / decimal, NTZ, shredding, blob encoding
  HoodieSchema fromPhysicalSchema(P physical, Option<HoodieSchema> hint);   // HoodieSchemaRepair folds in here
  TypeSupport support(HoodieSchemaType type, PhysicalOptions opts);
}

/** Catalog sync: table-level rules on top of a TypeSystemMapper. Types turn lossy only inside the mapper. */
public interface CatalogSchemaRenderer<C> {
  String catalog();
  C render(HoodieSchema schema, List<String> partitionFields, SyncOptions opts);
  SchemaDifference diff(HoodieSchema tableSchema, C current);
}
```

Implementations, and what each replaces:

| Interface | Implementation | Replaces |
|---|---|---|
| `TypeSystemMapper` | `SparkTypeMapper` | `HoodieSparkSchemaConverters`, `SparkFilterHelper.convertDataType`; `SparkInternalSchemaConverter` once RFC-104 retires `InternalSchema` |
| | `FlinkTypeMapper` | `HoodieSchemaConverter` (Flink) |
| | `HiveTypeMapper` | `HiveTypeUtils`; the type part of `HiveSchemaUtil` and the Flink catalog's `HiveSchemaUtils` |
| | `TrinoTypeMapper` | column types read from HMS Hive type strings |
| | `BigQueryTypeMapper` | the type switch in `BigQuerySchemaResolver` |
| `FormatSchemaMapper` | `ParquetSchemaMapper` | the parquet-avro converter copy, Flink `ParquetSchemaConverter`, the row writer's own derivation |
| | `ArrowSchemaMapper` (Lance) | Spark `LanceArrowUtils` path, `HoodieFlinkLanceArrowUtils` |
| | `AvroSchemaMapper` | `HoodieSchema.toAvroSchema()` |
| | `OrcSchemaMapper` | `AvroOrcUtils` |
| `CatalogSchemaRenderer` | `HiveCatalogRenderer` (HMS, Glue, ADB, Flink `HoodieHiveCatalog`) | `HiveSchemaUtil`, the HMS schema diff |
| | `SparkPropsRenderer` | `SparkSchemaUtils` (schema JSON) and `SparkDataSourceTableUtils` (`spark.sql.sources.schema.*` properties) |
| | `BigQueryRenderer` | the table part of `BigQuerySchemaResolver` |

Mappers are discovered with `ServiceLoader` through a `SchemaMappers` registry, so engine and sync modules plug in
without `hudi-common` depending on them.

The logical types RFC-99 specified but did not implement (TINYINT, SMALLINT, unsigned integers, FLOAT16, LARGE_STRING,
LARGE_BINARY, DATE64, DURATION, DICTIONARY) land here, each together with its declaration in every mapper and its rows
in the shared tests. A type is not done until every mapper declares it. Each new type also specifies how it persists in
the Avro JSON schema (a logical-type property old readers ignore) so older readers degrade to the base type.

### 2. Reader / writer

![Proposed: file reader and writer interfaces](writer-reader-proposal.svg)

#### 2.1 One row type for formats, wrapped by engines

Formats read and write a single row type, `HoodieRow`, and each engine wraps its own rows in both directions. This is the
model Apache Paimon uses (`InternalRow`, with `FlinkRowWrapper` and `FlinkRowData` as the Flink adapters). It grows the
`StructLike` interface that `hudi-common` already uses for metadata-table index evaluation into the read / write row,
with typed getters so values are not boxed.

![Engines x formats: N x M classes, or N + M through one row type](engines-x-formats.svg)

```java
/** The one row type file formats read and write. Getters return canonical values. */
public interface HoodieRow {
  int getFieldCount();
  boolean isNullAt(int i);
  boolean getBoolean(int i);
  int getInt(int i);                         // INT8, INT16, INT32, TIME(millis)
  long getLong(int i);                       // INT64, TIME(micros)
  float getFloat(int i);
  double getDouble(int i);
  byte[] getBinary(int i);                   // STRING as UTF-8, BINARY, FIXED, UUID
  HoodieDecimal getDecimal(int i, int precision, int scale);   // unscaled long up to 18 digits, else unscaled bytes
  int getDate(int i);                        // epoch days
  long getTimestamp(int i, int precision);   // in the column's unit
  HoodieRow getRow(int i, int numFields);
  HoodieArray getArray(int i);               // same typed getters by index
  HoodieMap getMap(int i);                   // key and value HoodieArrays
}
```

Each engine provides one wrapper per direction. Wrappers are reused across rows (`replace(row)`, as Paimon's
`FlinkRowData` does), and conversions happen once per logical type inside them. Read views do not copy values; `copy()`
materializes a row when the engine buffers it:

```java
/** Write: an engine row seen as a HoodieRow. */
final class SparkRowWrapper implements HoodieRow {
  private InternalRow row;
  SparkRowWrapper replace(InternalRow row) { this.row = row; return this; }
  public byte[] getBinary(int i) { return row.getUTF8String(i).getBytes(); }
  public long getLong(int i) { return row.getLong(i); }
  ...
}

/** Read: a HoodieRow seen as an engine row, without copying. */
final class HoodieSparkRow extends InternalRow {
  private HoodieRow row;
  HoodieSparkRow replace(HoodieRow row) { this.row = row; return this; }
  public UTF8String getUTF8String(int i) { return UTF8String.fromBytes(row.getBinary(i)); }   // wraps, no copy
  public long getLong(int i) { return row.getLong(i); }
  ...
}
```

| Engine | Write wrapper (implements `HoodieRow`) | Read view |
|---|---|---|
| Spark | `SparkRowWrapper` over `InternalRow` | `HoodieSparkRow extends InternalRow` |
| Flink | `FlinkRowWrapper` over `RowData` | `HoodieFlinkRow implements RowData` |
| Avro / Java client | `AvroRowWrapper` over `IndexedRecord` | `HoodieAvroRow implements IndexedRecord` |
| Hive (read only) | n/a | copies into `ArrayWritable`; replaces Avro then `avroToArrayWritable` |
| Trino (read only) | n/a | appends into Block builders for merged reads; replaces `HudiAvroSerializer` |

Today's engine writers already copy at this boundary (Spark's Parquet writer calls `getUTF8String(i).getBytes()` and
builds decimals through `BigDecimal`; Flink's calls `getString(i).toBytes()`), so a wrapper costs no more than they do,
and an unscaled `long` decimal costs less.

#### 2.2 One reader and writer per format

```java
/** One implementation per file format. */
public interface FileFormatIO {
  HoodieFileFormat format();
  FileWriter newWriter(WriteTarget target, HoodieSchema schema, WriteOptions opts);   // StoragePath or OutputStream
  FileReader newReader(InputFile file, HoodieSchema required, ReadOptions opts);
}

public interface FileWriter extends Closeable {
  void write(HoodieRow row);
  long bytesWritten();
}

public interface FileReader extends Closeable {
  HoodieSchema fileSchema();
  ClosableIterator<HoodieRow> read(SchemaTransformPlan plan, Option<Expression> filter);
}
```

Inside each format, the original `StructWriter` / `ValueWriter` split stays: a format's struct writer walks the fields of
a `HoodieRow` and hands each value to a `ValueWriter` chosen from the physical schema that `FormatSchemaMapper` produced.
Because the row type is fixed, struct writers are not subclassed per engine:

```java
/** Parquet, for every engine. */
final class ParquetStructWriter implements FileWriter {
  private final ValueWriter[] writers;   // one per field, chosen from toPhysicalSchema(schema)
  private final RecordConsumer consumer; // Parquet's write handle

  public void write(HoodieRow row) {
    consumer.startMessage();
    for (int i = 0; i < writers.length; i++) {
      if (!row.isNullAt(i)) {
        writers[i].write(row, i, consumer);   // e.g. IntWriter: consumer.addInteger(row.getInt(i))
      }
    }
    consumer.endMessage();
  }
}
```

Readers mirror this: a format's struct reader fills a `HoodieRow` from the file and applies the `SchemaTransformPlan`
(file schema to required schema: keep, rename, promote, fill default, drop) and an optional filter.

| Implementation | Replaces |
|---|---|
| `ParquetFileFormatIO` | `HoodieAvroParquetWriter`, `HoodieSparkParquetWriter`, `HoodieInternalRowParquetWriter`, `HoodieRowDataParquetWriter` and their write supports; the row-based Parquet readers |
| `LanceFileFormatIO` | `HoodieSparkLanceWriter`, `HoodieRowDataLanceWriter` and both Lance readers |
| `OrcFileFormatIO` | `HoodieAvroOrcWriter` / `HoodieAvroOrcReader`; Spark and Flink can now write ORC |
| `HFileFormatIO` | `HoodieAvroHFileWriter` and the HFile readers; values stay Avro binary, encoded from rows |
| `AvroBinaryFormatIO` | Avro data blocks: rows encoded straight to Avro binary instead of converting each row to an Avro record (`getAvroBytes`) |
| `VortexFileFormatIO` | `HoodieSparkVortexWriter` / `HoodieSparkVortexReader`; available to every engine in JDK 17 builds, as Vortex is today |
| `ArrowRowWriter` (shared by Lance and Vortex) | lance-spark's `LanceArrowWriter`, Spark's `ArrowWriter`, Flink's `LanceRowDataWriter`: fills an Arrow `VectorSchemaRoot` from `HoodieRow` |

A `FileFormats` registry keyed by `HoodieFileFormat` alone replaces `HoodieIOFactory`, which keys on record type x file
format. `HoodieFileFormat` gains an `AVRO` member for Avro data blocks; Parquet and HFile data blocks already hold
Parquet and HFile bytes. A missing combination becomes a missing implementation rather than a runtime exception.

#### 2.3 Engine fast paths

Engine batch readers stay: Spark's vectorized Parquet and Arrow readers, Flink's vectorized Parquet reader and Trino's
Page source. They sit behind an optional interface, must read with the same physical schema and plan, and must return
the same rows as the row reader:

```java
public interface VectorizedFileReader<B> extends Closeable {
  ClosableIterator<B> readBatches(SchemaTransformPlan plan, Option<Expression> filter);
}
```

#### 2.4 Log blocks

Parquet log blocks and native (v2) log files use `FileFormatIO` writers with an `OutputStream` target, as native logs
already do through the factory today. Avro and HFile log blocks keep their Avro encoding on disk, but rows are encoded
straight to Avro binary, so there is no `GenericRecord` per record. Reading them decodes straight into a `HoodieRow`,
which engines see through their read view, instead of decoding to `IndexedRecord` and converting each record again.
Merging stays in the FileGroupReader and its `RecordContext`.

The full path for an Avro data block (inline log format, write version below 10):

##### Write (Spark; Flink is the same with its own converter)

Today:

1. The Spark task produces an `InternalRow`, wrapped as a `HoodieSparkRecord`.
2. `HoodieAppendHandle` buffers the records and builds a `HoodieAvroDataBlock`.
3. `HoodieAvroDataBlock.serializeRecords` loops over them and calls `record.getAvroBytes(schema, props)` on each.
4. `HoodieSparkRecord.getAvroBytes` runs `AvroConversionUtils.createInternalRowToAvroConverter(...).apply(row)`, which
   produces a new `GenericRecord`. **Full copy 1.** Flink does the same via
   `RowDataQueryContext.getRowDataToAvroConverter().convert(...)`.
5. `HoodieAvroUtils.avroToBytesStream(record)` encodes it with `GenericDatumWriter` + `BinaryEncoder`. **Full copy 2: the
   encode.**
6. The bytes are appended to the block, and the block is written to the log file.

Proposed:

1. Same: `InternalRow`.
2. Same: append handle, `AvroBinaryFormatIO` writer on an `OutputStream`.
3. `sparkRowWrapper.replace(row)`, which only swaps a pointer.
4. Avro's struct writer walks the fields, calling `row.getInt(i)`, `row.getBinary(i)` and so on through the wrapper, and
   writes each value straight to the `BinaryEncoder`. **Single full copy: the encode.**
5. Same bytes, same block, same log file.

Result: the throwaway `GenericRecord` from step 4 disappears.

##### Read (file group reader merging a log block, Spark)

Today:

1. `HoodieAvroDataBlock.deserializeRecords(readerContext, content)`.
2. `RecordIterator` calls `GenericDatumReader.read(BinaryDecoder)` and gets an `IndexedRecord`. **Full copy 1: the
   decode.**
3. `RecordContext.convertAvroRecord(record)` produces an `InternalRow` (in `SparkFileFormatInternalRecordContext`).
   **Full copy 2.**
4. `projectLogBlockRecords`, then the file group reader merges against the base-file rows.

Proposed:

1. The `AvroBinaryFormatIO` reader decodes `BinaryDecoder` fields straight into a `HoodieRow`. **Single full copy: the
   decode.**
2. `new HoodieSparkRow(hoodieRow)` exposes it as an `InternalRow` view, without copying.
3. Same projection and merge.

Result: the `convertAvroRecord` copy from step 3 disappears.

Bytes on disk are identical. Because the FileGroupReader buffers log records by key for merging, the reader allocates a
new `HoodieRow` and view per record rather than reusing one, so values are still copied once. Where an engine needs its
own serialized form to spill buffered records (Spark's `UnsafeRow`), that conversion is outside this change.

#### 2.5 Alternatives considered

* **Per-engine subclasses of each format's struct writer and reader** (`SparkXFormatStructWriter`,
  `FlinkXFormatStructWriter`, ...), as sketched in the first version of this RFC. Once applied to every format this is
  one class per engine x format, and the engine's getter code is repeated per format.
* **Iceberg's `FormatModel` registry**: one registered model per (format, engine data type) pair, built for direct
  conversion without an intermediate row. It keeps engine x format implementations by design; the drift listed in the
  background is what that shape produces in Hudi.
* **Delta Kernel's engine handlers**: the engine reads and writes files with its own code. Adopted only for the
  vectorized fast paths above.
* **Keep each engine's own writers and remove only the Avro step** (vinothchandar's review of the first version: a Flink
  Parquet writer should write `RowData` without conversions). This is today's shape minus Avro; it keeps one writer per
  engine x format and the drift listed in the background. The copy a wrapper makes is the one engine writers already
  make (2.1).

### 3. HoodieRecord and KeyGenerator

Engine-specific `HoodieRecord` implementations keep engine rows on the write path (RFC-46). Spark, Hive and Flink
(`HoodieFlinkRecord`) now have them.

The base `KeyGenerator.getKey` takes an Avro `GenericRecord`; Spark (`SparkKeyGeneratorInterface`, over `InternalRow`)
and Flink (`RowDataKeyGen`, over `RowData`) each added their own row-based variant beside it. One generic interface
replaces the three:

```java
public interface KeyGenerator<R> extends Serializable {
  /**
   * Extract HoodieKey from the given engine specific row.
   */
  HoodieKey getKey(R record);
}
```

With `HoodieRow` in place, a key generator can also be written once against `HoodieRow` and serve every engine through
the write wrappers; that is a follow-up. Key fields defined as functions of columns (the `Expression` operators in 4.1)
build on the same follow-up.

### 4. Expressions

![Proposed: expression interfaces](expression-proposal.svg)

#### 4.1 Expression

`Expression` in `hudi-common` stays the one representation. Its operators are extended with functions and aggregates
for expression indexes, as proposed originally:

```java
public interface Expression extends Serializable {
  enum Operator {
    TRUE("TRUE", "true"),
    FALSE("FALSE", "false"),
    AND("AND", "&&"),
    OR("OR", "||"),
    GT(">", ">"),
    ...
    // date function
    DAY("DAY", "day"),
    HOUR("HOUR", "hour"),
    ...
    // string function
    UPPER("UPPER", "upper"),
    TRIM("TRIM", "trim"),
    ...
    // aggregate function
    COUNT("COUNT", "count"),
    MAX("MAX", "max"),
    MIN("MIN", "min")
  }
}
```

Literal and column types move from `InternalSchema` types to `HoodieSchemaType`, mapped by the same `TypeSystemMapper`
as columns. The `ExpressionVisitor` and `StructLike` from the original proposal stay; `StructLike` grows into `HoodieRow`
(section 2.1).

#### 4.2 Translating in and rendering out

```java
/** Engine filter to Hudi Expression: Spark, Flink, Hive, Trino. */
public interface ExpressionTranslator<E> {
  String system();
  Option<Expression> toHudi(E expr, HoodieSchema schema);
  ExpressionSupport support(Operator op, HoodieSchemaType type);
}

/** Hudi Expression to a catalog or reader filter: HMS, Glue, Parquet FilterPredicate, Spark Filter. */
public interface ExpressionRenderer<T> {
  String target();
  Option<T> render(Expression expr, MappingOptions opts);
}
```

Rules:

1. Translation and rendering are conservative: the result keeps every row the original keeps. NOT is first pushed down
   to the leaves; then an untranslatable AND conjunct is dropped, an OR with an untranslatable side is dropped whole, and
   a NOT over an untranslatable leaf is dropped.
2. A cast is stripped only if it preserves order; otherwise the predicate is dropped.
3. `support()` declares what each system translates; unsupported shapes are dropped, not guessed.

| Implementation | Replaces |
|---|---|
| `SparkTranslator` | `SparkFilterHelper`, the translation half of `DataSkippingUtils`, the key extraction in the record index and bloom support |
| `FlinkTranslator` | `ExpressionUtils` and evaluator building in `ExpressionEvaluators` |
| `HiveTranslator` | new; gives Hive metadata-table data skipping |
| `TrinoTranslator` | the `Domain` predicates in the `Hudi*IndexSupport` classes |
| `HmsRenderer`, `GlueRenderer` | `FilterGenVisitor`, `GlueFilterGenVisitor` |
| `ParquetRenderer` | Flink `ExpressionPredicates`; usable by any parquet-mr reader |
| `SparkFilterRenderer` | feeds Spark's vectorized Parquet reader |

#### 4.3 Integration with metadata-table indexes

Partition pruning evaluates the Hudi `Expression` once in `hudi-common`
(`HoodieTableMetadata.getPartitionPathWithPathPrefixUsingFilterExpression`, which exists today) for every engine. Data
skipping goes through one planner:

```java
public interface IndexPlanner {
  List<IndexLookup> plan(Expression filter, HoodieSchema schema, Set<MetadataPartitionType> available);   // order overridable per engine
}

/** One per index type: column stats, partition stats, expression index, record index, bloom filters, secondary index. */
public interface IndexLookup {
  MetadataPartitionType index();
  CandidateFiles lookup(Expression filter, HoodieTableMetadata mdt, StatsEvaluator eval, HoodieEngineContext ctx);   // ctx distributes key probes
}

/** How stats-based lookups evaluate the filter. */
public interface StatsEvaluator {
  CandidateFiles prune(Expression filter, MetadataPartitionType index, HoodieTableMetadata mdt);
}
```

`StatsEvaluator` has two implementations. `HudiStatsEvaluator` evaluates in `hudi-common` over metadata-table stats read
as `StructLike` (the `Evaluator` from the original proposal). Engine-native evaluators, such as a `SparkStatsEvaluator`
over DataFrames, keep engine-specific execution where it is faster. Every engine gets the same index coverage either
way, because the lookups are shared.

### Relationship to other RFCs

* **RFC-99** defines `HoodieSchema`, the type set and the interoperability table; this RFC implements the mapping
  layer against that table and the types it left open.
* **RFC-104** owns field ids, schema evolution and retiring `InternalSchema`. This RFC prepares for it: expressions and
  filter type mapping move from `InternalSchema` types to `HoodieSchemaType`, and callers go through the mappers, so
  once field ids live on `HoodieSchema` the remaining users of `InternalSchema` are its evolution history and id-based
  projection. `SchemaTransformPlan` takes renames from field ids once they exist.
* **RFC-115** (Spark DataFrame-native write path) and **RFC-87** (Avro elimination for the Flink writer) touch the same
  write paths; their writers become users of `FileFormatIO`.

## Rollout/Adoption Plan

1. **Mappers.** Add `TypeSupport`, the mapper interfaces and the registry, with implementations that wrap today's
   converters. No behaviour or on-disk change. Shared-test failures (INT16, UUID, DECIMAL, VECTOR) become the bug list.
2. **Converge mappings.** Callers use the mappers; the duplicated converters are deleted; repair moves into the format
   mappers; Trino takes its columns from `HoodieSchema`; the remaining RFC-99 types land.
3. **`HoodieRow` with its first format.** Add `HoodieRow`, the Spark, Flink and Avro wrappers and read views, and ORC
   through `OrcFileFormatIO` (write and read). Spark and Flink gain ORC; there is no engine ORC path to regress.
4. **One format at a time.** Avro and HFile log blocks (inline log format), then Parquet behind a benchmark against
   today's writers and readers (base files and native logs, the default path), then Lance and Vortex. Old writers and
   readers stay until their replacement lands; engine vectorized readers move behind `VectorizedFileReader` without being
   rewritten.
5. **Expressions**, independent of steps 3 and 4. Translators and renderers, then the index planner and lookups; Hive
   gains data skipping.

Each step ships on its own. No migration is needed for existing tables.

## Test Plan

* **Mapper contract tests**: one abstract suite per mapper interface that every implementation extends; round-trips
  every type x nullability x nesting x options; `support()` must match behaviour; RFC-99's interoperability table is the
  expected output.
* **Format contract tests**: every `FileFormatIO` with every engine wrapper writes and reads back every type; files
  written by different engines are byte-identical for the same rows.
* **Cross-engine golden tables**: write with each engine, read with every other.
* **Expression oracle**: for every engine, rows returned with pruning equal rows returned without it.
* **Benchmarks**: write and read throughput per format against today's engine writers and readers before each switch.

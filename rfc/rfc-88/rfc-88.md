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
* Replacing engine vectorized readers, or merging Flink's per-version copies of its vectorized Parquet reader; they
  only move behind `VectorizedFileReader` (2.3).

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
   `UNSUPPORTED`. Writing an unsupported type fails fast. `LOSSY` may drop type identity, never values; a format that
   cannot store a type's values exactly declares it `UNSUPPORTED`.
3. `fromX(toX(s))` equals `s` for every `NATIVE` type. For `LOSSY` types, the hint (the table schema) or a marker
   stored in the target restores the type, as described below.
4. Every mapper passes the same shared test kit, and RFC-99's interoperability table is its expected output.
5. Types are added to `HoodieSchema` first, then to every mapper. This rule is not negotiable. Two tiers:
   * Anything that affects values, physical layout, comparison or ordering (integer width, signedness, decimal
     precision and scale, timestamp unit, time zone semantics, collation, CHAR padding) is a `HoodieSchemaType`, which
     every mapper must declare. An engine type that needs one and has none fails fast in `fromTypeSystem` instead of
     widening.
   * Details that affect none of these (VARCHAR length, a timestamp's declared precision within its unit such as Flink
     TIMESTAMP(4) stored as micros, engine column options) are per-field properties on `HoodieSchema`
     (`HoodieSchemaField.getProp` / `addProp`). Every mapper preserves them and may ignore them.
     VARCHAR(n) qualifies because its values, bytes and ordering are those of STRING; only the write-time length check
     is engine-specific. CHAR(n) does not: Spark and Hive pad it and compare it ignoring trailing spaces.

   Storing a semantic distinction in a property of a wider type (SMALLINT as INT32 plus an engine annotation) is not
   allowed: a property one mapper reads and another ignores is how Spark and Flink write the same SMALLINT column
   differently today, and every such property becomes an unwritten type that each new mapper must rediscover. With this
   rule, nothing is lost on the way in and writers never need the engine schema.

**Why writers take two schemas today.** Each carries something the other lacks. The engine schema has integer width:
Spark's `ByteType` and `ShortType` become INT in `HoodieSchema`, so the row writer reads `ShortType` from `StructType` to
write INT(16), while Flink, deriving from `HoodieSchema`, writes plain INT32. `HoodieSchema` has timestamp millis,
decimal byte width, VECTOR, VARIANT and BLOB, which the Spark writer reads from it. Rule 5 moves the first group into
`HoodieSchema`; the second is already there, and only the mappers read it. Writers then take `HoodieSchema` alone.

**Where identity is lost.** Values are never lost (rule 2). Type identity is, and only on arrows that leave
`HoodieSchema`:

```
Write:  Engine --(1)--> HoodieSchema --(2)--> Format
Read:   Format --(3)--> HoodieSchema --(4)--> Engine
Sync:                   HoodieSchema --(5)--> Catalog
```

| Arrow | Lossy | Cases on master |
|---|---|---|
| (1) Engine to `HoodieSchema` | never (rule 5) | restores what (4) dropped |
| (2) `HoodieSchema` to format | a few types | Parquet: VECTOR as FIXED_LEN_BYTE_ARRAY, BLOB as a plain group, UUID as string (the UUID annotation is off by default). Avro: INT16 as `int` once INT16 lands |
| (3) Format to `HoodieSchema` | never | restores what (2) dropped |
| (4) `HoodieSchema` to engine | most cases | Spark: TIMESTAMP(3) as micros, TIME as int / long, UUID and ENUM as string, FIXED as binary, VECTOR as array, BLOB as struct. Flink: the same, except timestamps and TIME keep their precision |
| (5) `HoodieSchema` to catalog | some types | Hive: VECTOR, UUID and FIXED as binary, ENUM as string, BLOB as struct. Never read back, so nothing to restore |

**How identity comes back.** On (1) and (3), `fromTypeSystem(x, hint)` and `fromPhysicalSchema(x, hint)` resolve each
column in this order:

1. `NATIVE`: the target's own type is exact, such as Parquet INT(16) or TIMESTAMP(MILLIS).
2. `LOSSY` with a hint: the hint is the table schema, including its per-field properties, which Hudi already persists
   as `HoodieSchema` JSON: `schema` in
   each commit's metadata, `hoodie.table.create.schema` in `hoodie.properties`, the `SCHEMA` header of each log block,
   and `hoodie.avro.schema` in the write config during a write. It never crosses a lossy arrow, so it restores the
   exact type. Every Hudi read and write has one.
3. `LOSSY` without a hint, as in `CREATE TABLE` from engine DDL or a reader without the timeline: a per-column
   `hudi_type` marker stored in the target. Today Spark keeps it in `StructField` metadata (VECTOR, BLOB), and Parquet
   and Lance keep `hoodie.vector.columns` in the file footer (VECTOR only). Every `LOSSY` declaration names its marker,
   and targets with field or file metadata write one for every `LOSSY` column. Flink types have no metadata, so Flink
   relies on the hint.

The shared test kit checks `from(to(s))` equals `s` for every `LOSSY` type, once with the hint and once with the marker
alone.

**Example: VECTOR(128, FLOAT), written and read by Spark.**

1. (1) Spark hands over `emb array<float>`. The hint says `emb` is VECTOR(128, FLOAT), so `SparkTypeMapper` returns
   VECTOR. On `CREATE TABLE`, before any hint exists, the `hudi_type` marker on the `StructField` says the same.
2. (2) `ParquetSchemaMapper` writes FIXED_LEN_BYTE_ARRAY(512) and the footer marker `emb:VECTOR(128)` (FLOAT is the
   default). `LOSSY`: Parquet has no vector type, but all 512 bytes are kept.
3. (3) `ParquetSchemaMapper` sees FIXED_LEN_BYTE_ARRAY(512) and returns VECTOR(128, FLOAT) from the hint, or from the
   footer marker for a reader without the timeline. With neither, the column reads as FIXED(512), values intact.
4. (4) `SparkTypeMapper` returns `array<float>` with the `hudi_type` marker in the field metadata. `LOSSY`: Spark has no
   vector type, and the next write restores it in step 1.

A few more types, written and read back by Spark:

| Type | (1) Spark to `HoodieSchema` | (2) `HoodieSchema` to Parquet | (4) Spark reads back |
|---|---|---|---|
| SMALLINT | `ShortType` to INT16 (today INT) | INT32 + INT(16), the same from Flink | `ShortType`, `NATIVE` |
| TIMESTAMP(3) | `TimestampType`; the hint keeps millis | INT64 + TIMESTAMP(MILLIS), `NATIVE` | `TimestampType` (micros), `LOSSY` |
| DECIMAL(10,2) as fixed(8) | `DecimalType(10,2)`; the hint keeps the declared width | FIXED_LEN_BYTE_ARRAY(8), `NATIVE` | `DecimalType(10,2)`, `NATIVE` |
| Flink TIMESTAMP(9) | no `HoodieSchema` type: fails fast, as today | n/a until nanosecond TIMESTAMP lands | n/a |

Narrowing a value to the declared column type on write (Spark micros into a millis column) is a cast to the table
schema, as today, not a mapper loss.

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

The logical types RFC-99 specified but did not implement (TINYINT, SMALLINT, unsigned integers, FLOAT16, nanosecond
TIMESTAMP, LARGE_STRING, LARGE_BINARY, DATE64, DURATION, DICTIONARY) land here, each together with its declaration in
every mapper and its rows in the shared tests. CHAR(n) is added the same way, though RFC-99 did not specify it. Until a
type lands, engines keep today's mapping for it (CHAR to STRING with padded values, TINYINT and SMALLINT to INT); rule
5's fail-fast applies to a type only once its `HoodieSchemaType` exists (rollout step 2). A type is not
done until every mapper declares it. Each new type also specifies how it persists in the Avro JSON schema (a
logical-type property old readers ignore) so older readers degrade to the base type.

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
  long getDecimalUnscaledLong(int i, int precision, int scale);    // precision <= 18
  byte[] getDecimalUnscaledBytes(int i, int precision, int scale); // precision > 18, big-endian two's complement
  int getDate(int i);                        // epoch days
  long getTimestamp(int i, int precision);   // precision <= 6, in the column's unit
  HoodieTimestamp getNanoTimestamp(int i, int precision);   // precision 7-9: epoch millis + nano of milli, full range
  HoodieRow getRow(int i, int numFields);
  HoodieArray getArray(int i);               // same typed getters by index
  HoodieMap getMap(int i);                   // key and value HoodieArrays
}
```

Each engine provides one wrapper per direction. Wrappers are reused across rows (`replace(row)`, as Paimon's
`FlinkRowData` does), and so are the wrappers `getRow`, `getArray` and `getMap` return, one per nesting level.
Conversions happen once per logical type inside them. For nanosecond TIMESTAMP (section 1), a nanosecond `long` would
overflow outside roughly 1677-2262, so precision above 6 uses a two-part value, as Flink's `TimestampData` does. Read
views do not copy values:

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

Today's engine writers already make these conversions at this boundary: the Parquet and Lance writers copy strings out
(`getUTF8String(i).getBytes()` in Spark, `getString(i).toBytes()` in Flink), and all but Flink's Parquet writer build
decimals through `BigDecimal`. A wrapper moves those conversions rather than adding them, and an unscaled `long` decimal
costs less. ORC, HFile and Avro block writers convert every row to a `GenericRecord` first, which a wrapper removes.
Those paths get faster; for the direct engine writers (Spark and Flink Parquet and Lance) the target is parity, not a
gain. What a wrapper adds is one interface call per field; the benchmarks in rollout step 4 measure it.

On the read side, a `HoodieRow` is backed by what the reader decoded. Row formats (Avro and HFile blocks) decode into one
reused `GenericHoodieRow` per reader, an array of canonical values like Paimon's `GenericRow`. Columnar formats
(Parquet, Lance, ORC) expose a row view over the decoded column batch at a row index, like Spark's `ColumnarRow`, so no
row is materialized. Reuse is safe because a caller that keeps a row copies it: the file group reader's buffer does so
on every insert (`seal` / `toBinary`), as it already does for the reused row Spark's Avro deserializer returns today,
and `copy()` on a read view materializes a standalone engine row. Section 2.4 walks the full path.

Readers decode through a builder, so the decode target is pluggable without a per-engine decoder:

```java
/** What a format's struct reader writes decoded values into. */
public interface HoodieRowBuilder<R> {
  void setNull(int i);
  void setInt(int i, int v);
  void setLong(int i, long v);
  void setBinary(int i, byte[] v, int offset, int length);
  void setDecimalUnscaledLong(int i, long unscaled, int precision, int scale);
  ...                                    // one setter per HoodieRow getter
  R build();                             // reused; the caller copies a row it keeps
}
```

`GenericHoodieRow` is the default builder. An engine may supply its own, such as a Spark builder over `UnsafeRowWriter`
that hands `UnsafeRow` straight to the file group reader, so a reader can produce engine rows directly with one decoder
per format and one builder per engine. Columnar formats keep the batch view, which builds no row at all.

What each getter costs, for the write wrappers and read views alike:

| Getter | Returns | Allocation |
|---|---|---|
| `getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`, `getDate`, `getTimestamp`, `getDecimalUnscaledLong` | primitive | none beyond what the engine getter allocates (Flink's `DecimalData`, Spark's `Decimal` for non-`UnsafeRow` rows) |
| `getBinary`, `getDecimalUnscaledBytes` | `byte[]` valid until the next `replace` | the same copy today's writers make (`getUTF8String(i).getBytes()`, `toBytes()`, `toUnscaledBytes()`); read views over a decoded buffer return it without copying |
| `getRow`, `getArray`, `getMap` | a view, reused per nesting level, valid until the next call on its parent | none per value |
| `getNanoTimestamp` | `HoodieTimestamp` | one object per value, as Flink's `TimestampData` today |

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
| `ParquetFileFormatIO` | `HoodieAvroParquetWriter`, `HoodieSparkParquetWriter`, `HoodieInternalRowParquetWriter`, `HoodieRowDataParquetWriter` and their write supports; the Avro-based Parquet readers (`HoodieAvroParquetReader`) |
| `LanceFileFormatIO` | `HoodieSparkLanceWriter`, `HoodieRowDataLanceWriter` and both Lance readers |
| `OrcFileFormatIO` | `HoodieAvroOrcWriter` / `HoodieAvroOrcReader`; Spark and Flink can now write ORC |
| `HFileFormatIO` | `HoodieAvroHFileWriter` and the HFile readers; values stay Avro binary, encoded from rows |
| `AvroBinaryFormatIO` | Avro data blocks: rows encoded straight to Avro binary instead of converting each row to an Avro record (`getAvroBytes`) |
| `VortexFileFormatIO` | `HoodieSparkVortexWriter` / `HoodieSparkVortexReader`; available to every engine in JDK 17 builds, as Vortex is today |
| `ArrowRowWriter` (shared by Lance and Vortex) | lance-spark's `LanceArrowWriter`, Spark's `ArrowWriter`, Flink's `LanceRowDataWriter`: fills an Arrow `VectorSchemaRoot` from `HoodieRow` |

A `FileFormats` registry keyed by `HoodieFileFormat` alone replaces `HoodieIOFactory`, which keys on record type x file
format. `HoodieFileFormat` gains an `AVRO` member for Avro data blocks; Parquet and HFile data blocks already hold
Parquet and HFile bytes. A missing combination becomes a missing implementation rather than a runtime exception.

`HoodieFileWriter` stays the API that write handles call, with `HoodieRecord` and the record key as today:
`write(recordKey, record, schema)`, `writeWithMetadata`, `canWrite` and footer metadata. One implementation per format
sits on top of `FileFormatIO`. It keeps today's key, bloom-filter and rollover bookkeeping, prepends meta fields as a
view over the row (as `HoodieInternalRow` does for Spark today), and passes `record.asRow()` (section 3) to the format's
`FileWriter`. Only the encoding below that line is shared across engines.

#### 2.3 Engine fast paths

Engine batch readers stay: Spark's vectorized Parquet and Arrow readers, Flink's vectorized Parquet reader and Trino's
Page source. So do engine-native row readers that already build engine rows from the format without Avro, such as
`HoodieSparkParquetReader` over Spark's `ParquetReadSupport`; `HoodieRow` readers replace Hudi's own Avro-based, HFile,
log block, ORC and Lance readers, not engine code. Fast paths sit behind an optional interface, must read with the same
physical schema and plan, and must return the same rows as the row reader:

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
2. `RecordIterator` calls `GenericDatumReader.read(BinaryDecoder)` and gets a new `IndexedRecord`. **Full copy 1: the
   decode.**
3. `RecordContext.convertAvroRecord(record)` runs Spark's `AvroDeserializer` into a reused `InternalRow` (in
   `SparkFileFormatInternalRecordContext`). **Full copy 2: a second decode.**
4. `projectLogBlockRecords` passes rows through, projecting only when variant pushdown applies.
5. `KeyBasedFileGroupRecordBuffer.processNextDataRecord` inserts the record with `BufferedRecord.toBinary`:
   `UnsafeProjection` to `UnsafeRow`, then `seal` copies it (`InternalRow.copy()`). **Full copy 3, on every insert,
   spilled or not.**
6. The file group reader merges against the base-file rows.

Proposed:

1. Same: `deserializeRecords`.
2. The `AvroBinaryFormatIO` reader decodes `BinaryDecoder` fields straight into one reused `GenericHoodieRow`.
   **Full copy 1: the only decode.**
3. `HoodieSparkRow.replace(hoodieRow)` exposes it as an `InternalRow` view, without copying.
4. Same projection.
5. Same insert: `UnsafeProjection` reads through the view, then `seal` copies. **Full copy 2.**
6. Same merge.

Result: the `IndexedRecord` and the second decode from step 3 disappear. Nothing new is materialized; the view adds one
interface call per field while `UnsafeProjection` reads it. Bytes on disk are identical.

Reusing one `HoodieRow` is safe because step 5 copies every record it keeps. With a Spark `HoodieRowBuilder` over
`UnsafeRowWriter` (2.1), step 2 decodes straight into `UnsafeRow`, and step 5 skips its `UnsafeProjection`; the
benchmarks decide whether it ships alongside the default builder.

#### 2.5 Alternatives considered

* **Per-engine subclasses of each format's struct writer and reader** (`SparkXFormatStructWriter`,
  `FlinkXFormatStructWriter`, ...), as sketched in the first version of this RFC. Once applied to every format this is
  one class per engine x format, and the engine's getter code is repeated per format.
* **Iceberg's engine x format writers** (`SparkParquetWriters`, `FlinkParquetWriters`, `SparkOrcValueWriters`,
  `FlinkOrcWriters`, `SparkAvroWriter`, `FlinkAvroWriter`; the `FormatModel` registry indexes them): shared
  `ParquetValueWriters` cover primitives and the struct / repeated base classes, and each engine supplies its own writers
  for strings, UUIDs, three decimal encodings, timestamps, binary, arrays, maps and structs (about 550 lines per engine
  for Parquet alone), again for ORC and Avro. Compared on maintenance, conversion and performance:
  * Schema drift (the `INT(16)` annotation, three `MessageType` derivations) is fixed by the section 1 mappers in
    either model.
  * Maintenance: this model writes each engine's value code once per format; `HoodieRow` writes it once per engine.
    Every new format needs per-engine value code before an engine can write it, which is why Spark and Flink cannot
    write ORC today and Flink cannot write Vortex.
  * Conversion: the same in both. A wrapper runs the conversion the engine-specific value writer would run, once per
    value, without materializing a row (2.1, 2.4).
  * Performance: `HoodieRow` adds one interface call per field, measured against today's writers before each format
    switches (rollout step 4). Engine-native paths stay where a benchmark shows they matter (`VectorizedFileReader`,
    the row-builder follow-up in 2.4).
* **Delta Kernel's engine handlers**: the engine reads and writes files with its own code. Adopted only for the
  vectorized fast paths above.
* **Keep each engine's own writers and remove only the Avro step** (vinothchandar's review of the first version: a Flink
  Parquet writer should write `RowData` without conversions). This is today's shape minus Avro; it keeps one writer per
  engine x format, with the maintenance cost above. The copy a wrapper makes is the one engine writers already make
  (2.1).
* **`HoodieRecord` at the format boundary.** Three shapes: (a) keep `HoodieRecord` at `HoodieFileWriter` and give
  formats a `HoodieRow` view through `asRow()`, adopted (2.2, 3); (b) typed getters on `HoodieRecord` itself, rejected
  for now because nested structs, arrays and map entries still need a row type with no key or location, and abstract
  getters touch every `HoodieRecord` implementation, its Kryo serialization and payload code; (c) `HoodieRow` at the
  handle API, unnecessary since handles need the key and metadata `HoodieRecord` carries.

### 3. HoodieRecord and KeyGenerator

Engine-specific `HoodieRecord` implementations keep engine rows on the write path (RFC-46). Spark, Hive and Flink
(`HoodieFlinkRecord`) now have them. `HoodieRecord` stays the carrier at the writer boundary and gains one method, the
view formats read:

```java
public abstract class HoodieRecord<T> ... {
  /** The record's data as a HoodieRow; each engine record returns its write wrapper over getData(). */
  public abstract HoodieRow asRow();   // HoodieSparkRecord: sparkRowWrapper.replace(getData())
}
```

If `HoodieRecord` later gains typed getters of its own, it can implement `HoodieRow` and return itself from `asRow()`;
no format changes.

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
2. **Converge mappings.** First the missing types (INT8, INT16, CHAR and the remaining RFC-99 types) land in
   `HoodieSchema` and every mapper. Then callers switch to the mappers, one path at a time; the duplicated converters
   are deleted; repair moves into the format mappers; Trino takes its columns from `HoodieSchema`. A path keeps today's
   converter, and its writer keeps the engine schema, until the types it relies on have landed, so no engine schema
   accepted on master is rejected and no writer loses information it uses today.
3. **`HoodieRow` with its first format.** Add `HoodieRow`, the Spark, Flink and Avro wrappers and read views, and ORC
   through `OrcFileFormatIO` (write and read). Spark and Flink gain ORC; there is no engine ORC path to regress.
4. **One format at a time.** Avro and HFile log blocks (inline log format), then Parquet behind a benchmark against
   today's writers and readers (base files and native logs, the default path), then Lance and Vortex. Old writers and
   readers stay until their replacement lands; engine vectorized readers move behind `VectorizedFileReader` and engine
   native row readers stay as they are, neither rewritten. An engine and format pair whose benchmark regresses keeps its
   existing direct path.
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
* **Compatibility tests**: every engine schema master accepts (Spark `ByteType` / `ShortType` / CHAR / VARCHAR, Flink
  TINYINT / SMALLINT / CHAR / VARCHAR / TIMESTAMP(0-6)) is still accepted after each rollout step, and tables written
  by master read back with the same values and file layouts.
* **Expression oracle**: for every engine, rows returned with pruning equal rows returned without it.
* **Benchmarks**: before each switch, per format and engine, against each existing direct writer and reader:
  throughput, CPU and allocated bytes per row, on decimal-heavy, string-heavy and nested workloads as well as a mixed
  one.

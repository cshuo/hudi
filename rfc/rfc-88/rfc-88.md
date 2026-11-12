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

The type plane (one mapper per boundary between `HoodieSchema` and each engine, format and catalog, and the types RFC-99
left open) has since moved to [RFC-104](https://github.com/apache/hudi/pull/18661), so it does not wait on the
reader / writer design here. The expression part of the first version is dropped. This RFC keeps the data plane: file
readers and writers, `HoodieRecord` and `KeyGenerator`.

## Abstract

On write, Hudi receives an engine row (`InternalRow`, `RowData`, `IndexedRecord`) and hands it to a file format
(Parquet, Lance, ORC, HFile, Avro log blocks). Today every engine x format pair is its own writer, a detour through Avro,
or missing, and the readers mirror that.

This RFC gives formats one row type to read and write, `HoodieRow`, which each engine wraps; one reader and one
writer per format; and the `HoodieRecord` / `KeyGenerator` changes on top. It builds on two other RFCs: `HoodieSchema`
(RFC-99) is the schema, and RFC-104's type plane decides the physical schema of every file. The type plane depends on
nothing here, so the two proceed in parallel.

![Overview: type plane and data plane](overview.svg)

## Background

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

## Goals

* One reader and one writer per file format, usable by every engine without per-record Avro conversion.

## Non-goals

* Rewriting existing files. Avro / HFile log blocks keep their encoding, and a format writes the same bytes for the
  same physical schema as today.
* Schema mapping (the type plane), schema evolution and field ids, owned by
  [RFC-104](https://github.com/apache/hudi/pull/18661).
* Translating engine filters into Hudi `Expression` and a shared metadata-table index planner, proposed in the first
  version of this RFC and dropped. Readers still accept an optional Hudi `Expression` filter, the one `hudi-common` has
  today.
* The record merger API (RFC-101). Payload-based and custom record merging still convert records to Avro; removing that
  belongs to payload deprecation (RFC-97) and the merger API.
* Replacing engine vectorized readers, or merging Flink's per-version copies of its vectorized Parquet reader; they
  only move behind `VectorizedFileReader` (1.3).

## Design

| Piece | Owner |
|---|---|
| `HoodieSchema`, the type set and RFC-99's interoperability table (the spec) | RFC-99 |
| Type plane: mappers, `TypeSupport`, catalog renderers, shared tests; the types RFC-99 left open; Avro-free internals | RFC-104 |
| Field ids, schema evolution | RFC-104 |
| Data plane: reader / writer, `HoodieRecord` / `KeyGenerator` | this RFC |

Readers and writers here take their physical schema from RFC-104's `FormatSchemaMapper`. RFC-104's first type-plane
step puts today's converters behind that interface, so this RFC does not wait for the type plane to converge.

### 1. Reader / writer

![Proposed: file reader and writer interfaces](writer-reader-proposal.svg)

What the reader / writer layer has to guarantee, whatever the engine, operation or format:

1. **The table schema decides the file.** Every write builds its file schema from the table's `HoodieSchema` through
   the format's `FormatSchemaMapper` (RFC-104). No writer reads an engine schema to decide what lands on disk, so
   `bulk_insert` and upsert, or Spark and Flink, cannot write the same column differently (RFC-104's SMALLINT case
   study shows what happens today).
2. **Each decision is made once.** How a type is stored in a format lives in that format's mapper and writer. How an
   engine reads or builds its own rows lives in that engine's wrapper and builder. Neither is repeated per engine and
   format pair.
3. **Behaviour is checked, not assumed.** Shared tests run every engine against every format, with RFC-99's table as
   the expected result.
4. **Nothing new to own.** The boundary between engine code and format code is a transient view. Rows that are kept,
   spilled or shuffled stay engine rows, with the engine's serializers and memory accounting.

#### 1.1 One boundary between engine code and format code

Properties 1 and 2 need one boundary that format code reads and writes through, and that every engine implements for
its own rows. Formats read values through `HoodieRow` (writes) and write values through `HoodieRowBuilder` (reads). A
typed-getter boundary is a common shape: Spark's own Parquet and Arrow writers read through `SpecializedGetters`, which
both `InternalRow` and `ArrayData` implement; Iceberg's generic data path uses `StructLike`; Paimon's engine adapters
(`FlinkRowWrapper`, `FlinkRowData`) wrap its `InternalRow`. This RFC takes only that boundary, not Paimon's binary rows,
memory segments, serializers or format readers (1.5). It grows the `StructLike` interface that `hudi-common` already uses
for metadata-table index evaluation, with typed getters so values are not boxed.

`HoodieRow` replaces only code Hudi owns: every writer, and Hudi's own readers. Engine-owned read fast paths stay beside
it (1.3), bound to the same physical schema and plan. A consequence is one adapter per engine and one encoder per
format, instead of one per pair:

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
  byte[] getBinary(int i);                   // STRING as UTF-8, BINARY, FIXED, UUID; may copy
  void getBinaryRef(int i, BinaryRef ref);   // same value as a slice (buffer, offset, length); no copy
  long getDecimalUnscaledLong(int i, int precision, int scale);    // precision <= 18
  byte[] getDecimalUnscaledBytes(int i, int precision, int scale); // precision > 18, big-endian two's complement
  int getDate(int i);                        // epoch days
  long getTimestamp(int i, int precision);   // precision <= 6, in the column's unit
  long getNanoTimestampMillis(int i);        // precision 7-9: epoch millis ...
  int getNanoTimestampNanoOfMilli(int i);    // ... plus nano of milli, full range
  HoodieRow getRow(int i, int numFields);
  HoodieArray getArray(int i);               // same typed getters by index
  HoodieMap getMap(int i);                   // key and value HoodieArrays
}
```

Each engine provides a write wrapper, a read builder and a read view. Wrappers are reused across rows (`replace(row)`),
and so are the wrappers `getRow`, `getArray` and `getMap` return, one per nesting level.
Conversions happen once per logical type inside them. For nanosecond TIMESTAMP (a type RFC-104 adds), a nanosecond
`long` would overflow outside roughly 1677-2262, so precision above 6 uses a two-part value, as Flink's `TimestampData`
does. Read views do not copy values:

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

| Engine | Write wrapper (implements `HoodieRow`) | Read: builder for row formats | Read: view over column batches |
|---|---|---|---|
| Spark | `SparkRowWrapper` over `InternalRow` | over `UnsafeRowWriter` | `HoodieSparkRow extends InternalRow` |
| Flink | `FlinkRowWrapper` over `RowData` | over `BinaryRowWriter` | `HoodieFlinkRow implements RowData` |
| Avro / Java client | `AvroRowWrapper` over `IndexedRecord` | over `GenericData.Record` | `HoodieAvroRow implements IndexedRecord` |
| Hive (read only) | n/a | over `ArrayWritable`; replaces Avro then `avroToArrayWritable` | copies into `ArrayWritable` |
| Trino (read only) | n/a | appends into Block builders; replaces `HudiAvroSerializer` | appends into Block builders |

Today's engine writers already make these conversions at this boundary: the Parquet and Lance writers copy strings out
(`getUTF8String(i).getBytes()` in Spark, `getString(i).toBytes()` in Flink), and all but Flink's Parquet writer build
decimals through `BigDecimal`. A wrapper moves those conversions rather than adding them, and an unscaled `long` decimal
costs less. ORC, HFile and Avro block writers convert every row to a `GenericRecord` first, which a wrapper removes.
Those paths get faster; for the direct engine writers (Spark and Flink Parquet and Lance) the target is parity, not a
gain. What a wrapper adds is one interface call per field; the benchmarks in rollout step 2 measure it.

On the read side, row formats (Avro and HFile blocks) decode through a builder straight into the engine's own row, and
columnar formats (Parquet, Lance, ORC) expose a view over the decoded column batch at a row index, like Spark's
`ColumnarRow`. Neither materializes a Hudi row. Section 1.4 walks the full path.

`HoodieRow` is transient: a view over an engine row or a decoded batch, with no serializer, no stable binary layout, and
never spilled, shuffled or retained. Ownership follows from that:

* Views and the values they return (including `BinaryRef` slices and nested views) are borrowed, valid until the next
  `replace`, `next()` or call on their parent.
* A caller that keeps a row copies it into an engine row, through a builder or `copy()`. The file group reader's buffer
  already does so on every insert (`seal` / `toBinary`). Retained rows are engine rows, owned by the engine and counted
  by its memory manager and spill controls.

Readers decode through a builder, so one decoder per format can produce every engine's row:

```java
/** What a format's struct reader writes decoded values into. */
public interface HoodieRowBuilder<R> {
  void setNull(int i);
  void setInt(int i, int v);
  void setLong(int i, long v);
  void setBinary(int i, byte[] v, int offset, int length);
  void setDecimalUnscaledLong(int i, long unscaled, int precision, int scale);
  void setNanoTimestamp(int i, long epochMillis, int nanoOfMilli);
  ...                                    // one setter per HoodieRow getter
  R build();                             // reused; the caller copies a row it keeps
}
```

Each engine supplies the builder for its own row: Spark over `UnsafeRowWriter`, Flink over `BinaryRowWriter`, Avro over
`GenericData.Record`, Hive over `ArrayWritable`, Trino over its Block builders. That is one decoder per format and one
builder per engine, with no intermediate row. `GenericHoodieRow`, an array of canonical values, exists only for tests
and as the reference implementation.

What each getter costs, for the write wrappers and read views alike:

| Getter | Returns | Allocation |
|---|---|---|
| `getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`, `getDate`, `getTimestamp`, `getDecimalUnscaledLong` | primitive | none beyond what the engine getter allocates (Flink's `DecimalData`, Spark's `Decimal` for non-`UnsafeRow` rows) |
| `getNanoTimestampMillis`, `getNanoTimestampNanoOfMilli` | primitives | none beyond the engine getter (Flink's `TimestampData`) |
| `getBinaryRef` | fills a reused `BinaryRef` with the buffer, offset and length, borrowed | none: Spark's `UTF8String` and column-batch views hand over their own buffer |
| `getBinary`, `getDecimalUnscaledBytes` | `byte[]`, borrowed | the same copy today's writers make (`getUTF8String(i).getBytes()`, `toBytes()`, `toUnscaledBytes()`); a copy when the value is a slice of a larger buffer, which is why format writers use `getBinaryRef` |
| `getRow`, `getArray`, `getMap` | a view, reused per nesting level, borrowed | none per value |

Read cost on Hudi's own readers: no serialization and no extra copy of the row, since row formats decode straight into
the engine's row and columnar views read the batch in place. What remains is call overhead: one interface call per field
into a builder, and two per field for a columnar view wrapped by an engine view (`HoodieSparkRow.getInt` calls the
`HoodieRow` view, which reads the column vector), against one in Spark's `ColumnarRow`. Against the Avro-based readers
these replace, reads still get faster (1.4). Lance has per-engine readers today, so it is the case to benchmark; a pair
that regresses keeps its existing reader.

#### 1.2 One reader and writer per format

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
  /** Decodes each row into the engine's own row through its builder. */
  <R> ClosableIterator<R> read(SchemaTransformPlan plan, Option<Expression> filter, HoodieRowBuilder<R> builder);
  /** Columnar formats: transient views over the decoded batch, wrapped by the engine's read view. */
  ClosableIterator<HoodieRow> readViews(SchemaTransformPlan plan, Option<Expression> filter);
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

Readers mirror this: a format's struct reader decodes into the engine's builder (or, for columnar formats, exposes
views over the batch) and applies the `SchemaTransformPlan` (file schema to required schema: keep, rename, promote, fill
default, drop) and an optional filter.

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
view over the row (as `HoodieInternalRow` does for Spark today), and passes `record.asRow()` (section 2) to the format's
`FileWriter`. Only the encoding below that line is shared across engines.

#### 1.3 Engine fast paths

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

#### 1.4 Log blocks

Parquet log blocks and native (v2) log files use `FileFormatIO` writers with an `OutputStream` target, as native logs
already do through the factory today. Avro and HFile log blocks keep their Avro encoding on disk, but rows are encoded
straight to Avro binary, so there is no `GenericRecord` per record. Reading them decodes straight into the engine's own
row through its builder, instead of decoding to `IndexedRecord` and converting each record again.
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
2. The `AvroBinaryFormatIO` reader decodes `BinaryDecoder` fields through Spark's `HoodieRowBuilder` into one reused
   `UnsafeRow`. **Full copy 1: the only decode.**
3. Same projection.
4. Same insert: `toBinaryRow` returns the `UnsafeRow` as it is, then `seal` copies it. **Full copy 2.**
5. Same merge.

Result: the `IndexedRecord`, the second decode and the `UnsafeProjection` disappear, and no Hudi row sits in between.
Bytes on disk are identical. Reusing one `UnsafeRow` is safe because step 4 copies every record it keeps.

#### 1.5 Alternatives considered

* **Per-engine subclasses of each format's struct writer and reader** (`SparkXFormatStructWriter`,
  `FlinkXFormatStructWriter`, ...), as sketched in the first version of this RFC. Once applied to every format this is
  one class per engine x format, and the engine's getter code is repeated per format.
* **Iceberg's engine x format writers** (`SparkParquetWriters`, `FlinkParquetWriters`, `SparkOrcValueWriters`,
  `FlinkOrcWriters`, `SparkAvroWriter`, `FlinkAvroWriter`; the `FormatModel` registry indexes them): shared
  `ParquetValueWriters` cover primitives and the struct / repeated base classes, and each engine supplies its own writers
  for strings, UUIDs, three decimal encodings, timestamps, binary, arrays, maps and structs (about 550 lines per engine
  for Parquet alone), again for ORC and Avro. Compared on maintenance, conversion and performance:
  * Schema drift (the `INT(16)` annotation, three `MessageType` derivations) is fixed by RFC-104's mappers in
    either model.
  * Maintenance: this model writes each engine's value code once per format; `HoodieRow` writes it once per engine.
    Every new format needs per-engine value code before an engine can write it, which is why Spark and Flink cannot
    write ORC today and Flink cannot write Vortex.
  * Conversion: the same in both. A wrapper runs the conversion the engine-specific value writer would run, once per
    value, without materializing a row (1.1, 1.4).
  * Performance: `HoodieRow` adds one interface call per field, measured against today's writers before each format
    switches (rollout step 2). Engine-native paths stay where a benchmark shows they matter (`VectorizedFileReader`,
    the row-builder follow-up in 1.4).
* **Delta Kernel's engine handlers**: the engine reads and writes files with its own code. Adopted only for the
  vectorized fast paths above.
* **Keep each engine's own writers and remove only the Avro step** (vinothchandar's review of the first version: a Flink
  Parquet writer should write `RowData` without conversions). This is today's shape minus Avro; it keeps one writer per
  engine x format, with the maintenance cost above. The copy a wrapper makes is the one engine writers already make
  (1.1).
* **Paimon's full data model.** Paimon began as Flink Table Store and carries Flink's data layer: `InternalRow` is
  based on Flink's code, with `BinaryRow`, `BinaryString`, `MemorySegment` memory management, `InternalRowSerializer`,
  and its own Parquet and ORC readers and column vectors. An LSM store needs that, since it sorts, spills and stores
  rows in its own binary form and every engine reads through its readers, which then have to keep pace with each
  engine's own. This RFC adopts only the adapter shape:
  * Typed getters and engine adapters: adopted (`HoodieRow`, wrappers, views).
  * Binary rows, memory segments, serializers: not adopted. Rows that are kept are engine rows (`BufferedRecord` /
    `RecordContext`), and `HoodieRow` is transient.
  * Format readers that every engine reads through: not adopted. Engine vectorized and native row readers stay (1.3),
    and readers decode into the engine's own row through its builder.
* **`HoodieRecord` at the format boundary.** Three shapes: (a) keep `HoodieRecord` at `HoodieFileWriter` and give
  formats a `HoodieRow` view through `asRow()`, adopted (1.2, 2); (b) typed getters on `HoodieRecord` itself, rejected
  for now because nested structs, arrays and map entries still need a row type with no key or location, and abstract
  getters touch every `HoodieRecord` implementation, its Kryo serialization and payload code; (c) `HoodieRow` at the
  handle API, unnecessary since handles need the key and metadata `HoodieRecord` carries.

### 2. HoodieRecord and KeyGenerator

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
the write wrappers; that is a follow-up.

### Relationship to other RFCs

* **RFC-99** defines `HoodieSchema`, the type set and the interoperability table.
* **RFC-104** owns the type plane, field ids and schema evolution. Format readers and writers here take their physical
  schema from its `FormatSchemaMapper`, and the shared tests use RFC-99's table as the expected result.
  `SchemaTransformPlan` takes renames from field ids once they exist.
* **RFC-115** (Spark DataFrame-native write path) and **RFC-87** (Avro elimination for the Flink writer) touch the same
  write paths; their writers become users of `FileFormatIO`.

## Rollout/Adoption Plan

Both steps need only RFC-104's first type-plane step (`FormatSchemaMapper` over today's converters), not its type
convergence.

1. **`HoodieRow` with its first format.** Add `HoodieRow`, the Spark, Flink and Avro wrappers and read views, and ORC
   through `OrcFileFormatIO` (write and read). Spark and Flink gain ORC; there is no engine ORC path to regress.
2. **One format at a time.** Avro and HFile log blocks (inline log format), then Parquet behind a benchmark against
   today's writers and readers (base files and native logs, the default path), then Lance and Vortex. Old writers and
   readers stay until their replacement lands; engine vectorized readers move behind `VectorizedFileReader` and engine
   native row readers stay as they are, neither rewritten. An engine and format pair whose benchmark regresses keeps its
   existing direct path.

Each step ships on its own. No migration is needed for existing tables.

## Test Plan

* **Format contract tests**: every `FileFormatIO` with every engine wrapper writes and reads back every type; files
  written by different engines are byte-identical for the same rows.
* **Cross-engine golden tables**: write with each engine, read with every other.
* **Compatibility tests**: after each rollout step, tables written by master read back with the same values, and a
  switched format writes the same file layouts as the writer it replaces.
* **Benchmarks**: before each switch, per format and engine, against each existing direct writer and reader:
  throughput, CPU, allocated bytes per row, GC time, retained heap and off-heap memory, and bytes copied, on long
  strings, wide decimals, nanosecond timestamps, large nested rows and a mixed workload.

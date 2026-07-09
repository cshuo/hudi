/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.hadoop.OrcReaderIterator;
import org.apache.hudi.io.storage.hadoop.OrcColumnStatsMetadata;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.metadata.stats.ValueMetadata;
import org.apache.hudi.metadata.stats.ValueType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto.UserMetadataItem;
import org.apache.orc.Reader;
import org.apache.orc.Reader.Options;
import org.apache.orc.RecordReader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.BinaryUtil.toBytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;

/**
 * Utility functions for ORC files.
 */
public class OrcUtils extends FileFormatUtils {

  /**
   * Provides a closable iterator for reading the given ORC file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The ORC file path
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the ORC file
   */
  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath) {
    return getHoodieKeyIterator(storage, filePath, Option.empty(), Option.empty());
  }

  /**
   * Fetch {@link HoodieKey}s from the given ORC file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The ORC file path.
   * @return {@link List} of {@link HoodieKey}s fetched from the ORC file
   */
  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath) {
    return fetchRecordKeysWithPositions(storage, filePath, Option.empty(), Option.empty());
  }

  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
    try {
      if (!storage.exists(filePath)) {
        return ClosableIterator.wrap(Collections.emptyIterator());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from ORC file:" + filePath, e);
    }
    AtomicLong position = new AtomicLong(0);
    return new CloseableMappingIterator<>(getHoodieKeyIterator(storage, filePath, keyGeneratorOpt, partitionPath), key -> Pair.of(key, position.getAndIncrement()));
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
    try {
      Configuration conf = storage.getConf().unwrapCopyAs(Configuration.class);
      conf.addResource(HadoopFSUtils.getFs(filePath.toString(), conf).getConf());
      Reader reader = OrcFile.createReader(convertToHadoopPath(filePath), OrcFile.readerOptions(conf));

      HoodieSchema readSchema = getKeyIteratorSchema(storage, filePath, keyGeneratorOpt, partitionPath);
      TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(readSchema);
      RecordReader recordReader = reader.rows(new Options(conf).schema(orcSchema));

      return HoodieKeyIterator.getInstance(
          new OrcReaderIterator<>(recordReader, readSchema, orcSchema), keyGeneratorOpt, partitionPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to open reader from ORC file:" + filePath, e);
    }
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath) {
    HoodieSchema schema;
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      schema = AvroOrcUtils.createSchema(reader.getSchema());
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read Avro records from an ORC file:" + filePath, io);
    }
    return readAvroRecords(storage, filePath, schema);
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, HoodieSchema schema) {
    List<GenericRecord> records = new ArrayList<>();
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      TypeDescription orcSchema = reader.getSchema();
      try (RecordReader recordReader = reader.rows(
          new Options(storage.getConf().unwrapAs(Configuration.class)).schema(orcSchema))) {
        OrcReaderIterator<GenericRecord> iterator = new OrcReaderIterator<>(recordReader, schema, orcSchema);
        while (iterator.hasNext()) {
          GenericRecord record = iterator.next();
          records.add(record);
        }
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to create an ORC reader for ORC file:" + filePath, io);
    }
    return records;
  }

  /**
   * Read the rowKey list matching the given filter, from the given ORC file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The ORC file path.
   * @param filter   record keys filter
   * @return Set Set of pairs of row key and position matching candidateRecordKeys
   */
  @Override
  public Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter)
      throws HoodieIOException {
    long rowPosition = 0;
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      TypeDescription schema = reader.getSchema();
      try (RecordReader recordReader = reader.rows(new Options(storage.getConf().unwrapAs(Configuration.class)).schema(schema))) {
        Set<Pair<String, Long>> filteredRowKeys = new HashSet<>();
        List<String> fieldNames = schema.getFieldNames();
        VectorizedRowBatch batch = schema.createRowBatch();

        // column index for the RECORD_KEY_METADATA_FIELD field
        int colIndex = -1;
        for (int i = 0; i < fieldNames.size(); i++) {
          if (fieldNames.get(i).equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
            colIndex = i;
            break;
          }
        }
        if (colIndex == -1) {
          throw new HoodieException(String.format("Couldn't find row keys in %s.", filePath));
        }
        while (recordReader.nextBatch(batch)) {
          BytesColumnVector rowKeys = (BytesColumnVector) batch.cols[colIndex];
          for (int i = 0; i < batch.size; i++) {
            String rowKey = rowKeys.toString(i);
            if (filter.isEmpty() || filter.contains(rowKey)) {
              filteredRowKeys.add(Pair.of(rowKey, rowPosition));
            }
            rowPosition++;
          }
        }
        return filteredRowKeys;
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read row keys for ORC file:" + filePath, io);
    }
  }

  @Override
  public Map<String, String> readFooter(HoodieStorage storage, boolean required,
                                        StoragePath filePath, String... footerNames) {
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      Map<String, String> footerVals = new HashMap<>();
      List<UserMetadataItem> metadataItemList = reader.getFileTail().getFooter().getMetadataList();
      Map<String, String> metadata = metadataItemList.stream().collect(Collectors.toMap(
          UserMetadataItem::getName,
          metadataItem -> metadataItem.getValue().toStringUtf8()));
      for (String footerName : footerNames) {
        if (metadata.containsKey(footerName)) {
          footerVals.put(footerName, metadata.get(footerName));
        } else if (required) {
          throw new MetadataNotFoundException(
              "Could not find index in ORC footer. Looked for key " + footerName + " in " + filePath);
        }
      }
      return footerVals;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read footer for ORC file:" + filePath, io);
    }
  }

  @Override
  public HoodieSchema readSchema(HoodieStorage storage, StoragePath filePath) {
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      if (reader.hasMetadataValue("orc.avro.schema")) {
        ByteBuffer metadataValue = reader.getMetadataValue("orc.avro.schema");
        byte[] bytes = toBytes(metadataValue);
        return HoodieSchema.parse(new String(bytes));
      } else {
        TypeDescription orcSchema = reader.getSchema();
        return AvroOrcUtils.createSchema(orcSchema);
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to get Avro schema for ORC file:" + filePath, io);
    }
  }

  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage, StoragePath filePath, List<String> columnList, HoodieIndexVersion indexVersion) {
    throw new UnsupportedOperationException(
        "Reading column statistics from an ORC file path is not supported yet; use the in-memory "
            + "OrcColumnStatsMetadata overload produced by the ORC writer instead");
  }

  /**
   * Builds column statistics from the in-memory ORC file format metadata captured by the writer,
   * mirroring {@code ParquetUtils#readColumnStatsFromMetadata(ParquetMetadata, ...)}. This avoids
   * re-reading the file: the file level {@link ColumnStatistics} are taken directly from the ORC
   * writer.
   *
   * @param metadata     ORC file format metadata (statistics + schemas) captured after write.
   * @param fileName     the file name to record on the resulting column range metadata.
   * @param columnList   optional set of columns to restrict collection to; all columns when empty.
   * @param indexVersion the column stats index version.
   */
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(OrcColumnStatsMetadata metadata,
                                                                                 String fileName,
                                                                                 Option<List<String>> columnList,
                                                                                 HoodieIndexVersion indexVersion) {
    ColumnStatistics[] columnStatistics = metadata.getColumnStatistics();
    TypeDescription orcSchema = metadata.getOrcSchema();
    HoodieSchema schema = metadata.getSchema();
    if (columnStatistics == null || columnStatistics.length == 0 || orcSchema.getCategory() != TypeDescription.Category.STRUCT) {
      return Collections.emptyList();
    }

    // The root struct statistics carry the total number of rows written to the file.
    long totalRows = columnStatistics[0].getNumberOfValues();
    Set<String> columnsToMatch = columnList.map(cols -> (Set<String>) new HashSet<>(cols)).orElse(null);

    List<TypeDescription> children = orcSchema.getChildren();
    List<String> fieldNames = orcSchema.getFieldNames();
    List<HoodieColumnRangeMetadata<Comparable>> result = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
      String columnName = fieldNames.get(i);
      if (columnsToMatch != null && !columnsToMatch.contains(columnName)) {
        continue;
      }
      TypeDescription fieldType = children.get(i);
      int columnId = fieldType.getId();
      Option<HoodieSchemaField> fieldOpt = schema.getField(columnName);
      if (columnId >= columnStatistics.length || !fieldOpt.isPresent()) {
        continue;
      }
      HoodieSchema fieldSchema = fieldOpt.get().schema();
      ValueMetadata valueMetadata;
      try {
        valueMetadata = ValueMetadata.getValueMetadata(fieldSchema, indexVersion);
      } catch (IllegalArgumentException e) {
        // Column has an unsupported (e.g. complex) type for the column stats index; skip it.
        continue;
      }
      ColumnStatistics colStats = columnStatistics[columnId];
      HoodieSchema valueSchema = fieldSchema.getNonNullType();
      Comparable<?> minValue = convertOrcColumnStat(fieldType, colStats, valueSchema, valueMetadata.getValueType(), true);
      Comparable<?> maxValue = convertOrcColumnStat(fieldType, colStats, valueSchema, valueMetadata.getValueType(), false);
      long nullCount = totalRows - colStats.getNumberOfValues();
      result.add((HoodieColumnRangeMetadata<Comparable>) HoodieColumnRangeMetadata.<Comparable>create(
          fileName,
          columnName,
          (Comparable) valueMetadata.standardizeJavaTypeAndPromote(minValue),
          (Comparable) valueMetadata.standardizeJavaTypeAndPromote(maxValue),
          nullCount,
          totalRows,
          0L,
          0L,
          valueMetadata));
    }
    return result;
  }

  /**
   * Converts an ORC {@link ColumnStatistics} min/max into a natural Java value that the column
   * stats {@link ValueMetadata} can standardize. Returns {@code null} when the column has no values
   * or the type has no meaningful min/max (e.g. binary/complex types).
   */
  private static Comparable<?> convertOrcColumnStat(TypeDescription fieldType,
                                                    ColumnStatistics colStats,
                                                    HoodieSchema valueSchema,
                                                    ValueType valueType,
                                                    boolean isMin) {
    if (colStats.getNumberOfValues() == 0) {
      return null;
    }
    switch (fieldType.getCategory()) {
      case BOOLEAN: {
        BooleanColumnStatistics stats = (BooleanColumnStatistics) colStats;
        if (isMin) {
          return stats.getFalseCount() > 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        return stats.getTrueCount() > 0 ? Boolean.TRUE : Boolean.FALSE;
      }
      case BYTE:
      case SHORT:
      case INT: {
        IntegerColumnStatistics stats = (IntegerColumnStatistics) colStats;
        return (int) (isMin ? stats.getMinimum() : stats.getMaximum());
      }
      case LONG: {
        IntegerColumnStatistics stats = (IntegerColumnStatistics) colStats;
        return isMin ? stats.getMinimum() : stats.getMaximum();
      }
      case FLOAT: {
        DoubleColumnStatistics stats = (DoubleColumnStatistics) colStats;
        return (float) (isMin ? stats.getMinimum() : stats.getMaximum());
      }
      case DOUBLE: {
        DoubleColumnStatistics stats = (DoubleColumnStatistics) colStats;
        return isMin ? stats.getMinimum() : stats.getMaximum();
      }
      case STRING:
      case CHAR:
      case VARCHAR: {
        StringColumnStatistics stats = (StringColumnStatistics) colStats;
        return isMin ? stats.getMinimum() : stats.getMaximum();
      }
      case DATE: {
        DateColumnStatistics stats = (DateColumnStatistics) colStats;
        java.util.Date date = isMin ? stats.getMinimum() : stats.getMaximum();
        if (date == null) {
          return null;
        }
        // ORC returns a java.sql.Date built with the same (local) convention that #toLocalDate uses,
        // which round-trips back to the original day.
        return date instanceof java.sql.Date ? (java.sql.Date) date : new java.sql.Date(date.getTime());
      }
      case TIMESTAMP: {
        TimestampColumnStatistics stats = (TimestampColumnStatistics) colStats;
        // Use the non-UTC accessor so the min/max match the values Hudi's ORC reader surfaces:
        // the reader reads TimestampColumnVector#time directly (writer-local millis), which
        // round-trips the written logical value, whereas getMinimumUTC() would apply an extra
        // timezone shift and break data skipping.
        java.sql.Timestamp ts = isMin ? stats.getMinimum() : stats.getMaximum();
        if (ts == null) {
          return null;
        }
        switch (valueType) {
          case LOCAL_TIMESTAMP_MILLIS:
          case LOCAL_TIMESTAMP_MICROS:
          case LOCAL_TIMESTAMP_NANOS:
            return LocalDateTime.ofInstant(ts.toInstant(), ZoneOffset.UTC);
          default:
            return ts.toInstant();
        }
      }
      case DECIMAL: {
        DecimalColumnStatistics stats = (DecimalColumnStatistics) colStats;
        HiveDecimal decimal = isMin ? stats.getMinimum() : stats.getMaximum();
        if (decimal == null) {
          return null;
        }
        BigDecimal bigDecimal = decimal.bigDecimalValue();
        if (valueSchema instanceof HoodieSchema.Decimal) {
          // HiveDecimal drops trailing zeros; enforce the schema scale so the value round-trips
          // through the metadata table decimal encoding (which assumes the schema scale).
          bigDecimal = bigDecimal.setScale(((HoodieSchema.Decimal) valueSchema).getScale());
        }
        return bigDecimal;
      }
      default:
        // BINARY / complex types have no meaningful min/max for the column stats index.
        return null;
    }
  }

  @Override
  public HoodieFileFormat getFormat() {
    return HoodieFileFormat.ORC;
  }

  @Override
  public long getRowCount(HoodieStorage storage, StoragePath filePath) {
    try (Reader reader = OrcFile.createReader(
        convertToHadoopPath(filePath), OrcFile.readerOptions(storage.getConf().unwrapAs(Configuration.class)))) {
      return reader.getNumberOfRows();
    } catch (IOException io) {
      throw new HoodieIOException("Unable to get row count for ORC file:" + filePath, io);
    }
  }

  @Override
  public void writeMetaFile(HoodieStorage storage, StoragePath filePath, Properties props) throws IOException {
    // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
    // parameters are not important.
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(storage.getConf().unwrapAs(Configuration.class))
        .fileSystem((FileSystem) storage.getFileSystem())
        .setSchema(AvroOrcUtils.createOrcSchema(schema));
    try (Writer writer = OrcFile.createWriter(convertToHadoopPath(filePath), writerOptions)) {
      for (String key : props.stringPropertyNames()) {
        writer.addUserMetadata(key, ByteBuffer.wrap(getUTF8Bytes(props.getProperty(key))));
      }
    }
  }

  @Override
  public ByteArrayOutputStream serializeRecordsToLogBlock(HoodieStorage storage,
                                                          List<HoodieRecord> records,
                                                          HoodieSchema writerSchema,
                                                          HoodieSchema readerSchema,
                                                          String keyFieldName,
                                                          Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("Hudi log blocks do not support ORC format yet");
  }

  @Override
  public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                        Iterator<HoodieRecord> records,
                                                                        HoodieRecordType recordType,
                                                                        HoodieSchema writerSchema,
                                                                        HoodieSchema readerSchema,
                                                                        String keyFieldName,
                                                                        Map<String, String> paramsMap) throws IOException {
    throw new UnsupportedOperationException("Hudi log blocks do not support ORC format yet");
  }
}

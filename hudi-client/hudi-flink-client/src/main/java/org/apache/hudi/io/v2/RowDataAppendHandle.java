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

package org.apache.hudi.io.v2;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.LogFileCreationCallback;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.collectColumnRangeMetadata;

/**
 * todo add doc
 */
public class RowDataAppendHandle<T, I, K, O> extends FlinkWriteHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataAppendHandle.class);
  private static final AtomicLong RECORD_COUNTER = new AtomicLong(1);

  private HoodieLogFormat.Writer writer;
  // Buffer for holding records (to be deleted), along with their position in log block, in memory before they are flushed to disk
  private final List<Pair<DeleteRecord, Long>> recordsToDeleteWithPositions = new ArrayList<>();
  // Header metadata for a log block
  protected final Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
  protected final List<HoodieRecord> recordList = new ArrayList<>();
  // Total number of records written during appending
  protected long recordsWritten = 0;
  // Total number of records deleted during appending
  protected long recordsDeleted = 0;
  // Total number of records updated during appending
  protected long updatedRecordsWritten = 0;
  // Total number of new records inserted into the delta file
  protected long insertRecordsWritten = 0;
  private boolean isClosed = false;

  public RowDataAppendHandle(
      HoodieWriteConfig config,
      RowType rowType,
      Option<String> instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String fileId,
      String partitionPath,
      TaskContextSupplier taskContextSupplier) {
    super(config, rowType, instantTime, hoodieTable, fileId, partitionPath, taskContextSupplier);
    this.writer = createLogWriter(this.instantTime, null);
  }

  /**
   * Append data and delete blocks into log file.
   */
  public WriteStatus appendRowData(Iterator<RowData> recordIterator) {
    initWriteStatus();
    prepareRecords(recordIterator);
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchemaWithMetaFields.toString());
      List<HoodieLogBlock> blocks = new ArrayList<>(2);
      if (!recordList.isEmpty()) {
        String keyField = config.populateMetaFields()
            ? HoodieRecord.RECORD_KEY_METADATA_FIELD
            : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();
        blocks.add(genDataBlock(config, getLogDataBlockFormat(), recordList, keyField, header));
      }

      if (!recordsToDeleteWithPositions.isEmpty()) {
        blocks.add(new HoodieDeleteBlock(recordsToDeleteWithPositions, header));
      }

      if (!blocks.isEmpty()) {
        AppendResult appendResult = writer.appendBlocks(blocks);
        processAppendResult(appendResult, recordList);
        recordList.clear();
        recordsToDeleteWithPositions.clear();
      }
    } catch (Exception e) {
      throw new HoodieAppendException("Failed while appending records to " + writer.getLogFile().getPath(), e);
    }
    return writeStatus;
  }

  public void initWriteStatus() {
    HoodieDeltaWriteStat deltaWriteStat = new HoodieDeltaWriteStat();
    deltaWriteStat.setPartitionPath(partitionPath);
    deltaWriteStat.setFileId(fileId);
    this.writeStatus.setFileId(fileId);
    this.writeStatus.setPartitionPath(partitionPath);
    this.writeStatus.setStat(deltaWriteStat);
  }

  private void prepareRecords(Iterator<RowData> recordIterator) {
    while (recordIterator.hasNext()) {
      HoodieRecord record = convertToRecord(recordIterator.next());
      if (!partitionPath.equals(record.getPartitionPath())) {
        HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
            + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
        writeStatus.markFailure(record, failureEx, Option.empty());
        continue;
      }
      boolean isDelete = HoodieOperation.isDelete(record.getOperation()) && !config.allowOperationMetadataField();
      if (isDelete) {
        final Comparable<?> orderingVal = record.getOrderingValue(writeSchema, config.getProps());
        recordsToDeleteWithPositions.add(Pair.of(DeleteRecord.create(record.getKey(), orderingVal), HoodieRecordLocation.INVALID_POSITION));
      } else {
        recordList.add(record);
      }
    }
  }

  private void processAppendResult(AppendResult result, List<HoodieRecord> recordList) {
    HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();

    Preconditions.checkArgument(stat.getPath() == null, "Only one append are expected for " + this.getClass().getSimpleName());
    // first time writing to this log block.
    updateWriteStatus(stat, result);

    // for parquet data block, we can get column stats from parquet footer directly.
    if (config.isMetadataColumnStatsIndexEnabled()) {
      Set<String> columnsToIndexSet = new HashSet<>(HoodieTableMetadataUtil
          .getColumnsToIndex(hoodieTable.getMetaClient().getTableConfig(),
              config.getMetadataConfig(), Lazy.eagerly(Option.of(writeSchemaWithMetaFields)),
              Option.of(HoodieRecord.HoodieRecordType.FLINK)).keySet());
      if (result.getRecordsStats().isEmpty()) {
        final List<Pair<String, Schema.Field>> fieldsToIndex = columnsToIndexSet.stream()
            .map(fieldName -> HoodieAvroUtils.getSchemaForField(writeSchemaWithMetaFields, fieldName)).collect(Collectors.toList());
        try {
          Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
              collectColumnRangeMetadata(recordList, fieldsToIndex, stat.getPath(), writeSchemaWithMetaFields);
          stat.putRecordsStats(columnRangeMetadataMap);
        } catch (HoodieException e) {
          throw new HoodieAppendException("Failed to extract append result", e);
        }
      } else {
        Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
            result.getRecordsStats().entrySet().stream()
                .filter(e -> columnsToIndexSet.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().copy(stat.getPath())));
        stat.putRecordsStats(columnRangeMetadataMap);
      }
    }
    resetWriteCounts();
    assert stat.getRuntimeStats() != null;
    LOG.info("AppendHandle for partitionPath {} filePath {}, took {} ms.",
        partitionPath, stat.getPath(), stat.getRuntimeStats().getTotalUpsertTime());
  }

  protected HoodieFlinkRecord convertToRecord(RowData dataRow) {
    boolean isPopulateMetaFields = config.populateMetaFields();
    boolean allowOperationMetadataField = config.allowOperationMetadataField();
    String key = rowDataKeyGen.getRecordKey(dataRow);
    Comparable<?> preCombineValue = rowDataKeyGen.getPreCombineValue(dataRow);
    HoodieOperation operation = HoodieOperation.fromValue(dataRow.getRowKind().toByteValue());
    HoodieKey hoodieKey = new HoodieKey(key, partitionPath);
    if (!isPopulateMetaFields && !allowOperationMetadataField) {
      return new HoodieFlinkRecord(hoodieKey, operation, preCombineValue, dataRow);
    }

    int metaArity = (isPopulateMetaFields ? 5 : 0) + (allowOperationMetadataField ? 1 : 0);
    GenericRowData metaRow = new GenericRowData(metaArity);
    if (isPopulateMetaFields) {
      String seqId = HoodieRecord.generateSequenceId(instantTime, getPartitionId(), RECORD_COUNTER.getAndIncrement());
      metaRow.setField(0, StringData.fromString(instantTime));
      metaRow.setField(1, StringData.fromString(seqId));
      metaRow.setField(2, StringData.fromString(key));
      metaRow.setField(3, StringData.fromString((partitionPath)));
      metaRow.setField(4, StringData.fromString(fileId));
    }
    if (allowOperationMetadataField) {
      metaRow.setField(5, HoodieOperation.fromValue(dataRow.getRowKind().toByteValue()).getName());
    }
    return new HoodieFlinkRecord(hoodieKey, operation, preCombineValue, new JoinedRowData(dataRow.getRowKind(), metaRow, dataRow));
  }

  private void updateWriteStatus(HoodieDeltaWriteStat stat, AppendResult result) {
    // update WriteStat
    stat.setPath(makeFilePath(result.logFile()));
    stat.setLogOffset(result.offset());
    stat.setLogVersion(result.logFile().getLogVersion());
    if (!stat.getLogFiles().contains(result.logFile().getFileName())) {
      stat.addLogFiles(result.logFile().getFileName());
    }
    stat.setFileSizeInBytes(result.size());

    // update write counts
    stat.setNumWrites(recordsWritten);
    stat.setNumUpdateWrites(updatedRecordsWritten);
    stat.setNumInserts(insertRecordsWritten);
    stat.setNumDeletes(recordsDeleted);
    stat.setTotalWriteBytes(result.size());

    // update runtime stats
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalUpsertTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
  }

  private String makeFilePath(HoodieLogFile logFile) {
    return partitionPath.isEmpty()
        ? new StoragePath(logFile.getFileName()).toString()
        : new StoragePath(partitionPath, logFile.getFileName()).toString();
  }

  private void resetWriteCounts() {
    recordsWritten = 0;
    updatedRecordsWritten = 0;
    insertRecordsWritten = 0;
    recordsDeleted = 0;
  }

  private HoodieLogBlock.HoodieLogBlockType getLogDataBlockFormat() {
    Option<HoodieLogBlock.HoodieLogBlockType> logBlockTypeOpt = config.getLogDataBlockFormat();
    if (logBlockTypeOpt.isPresent()) {
      return logBlockTypeOpt.get();
    }
    return HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  @Override
  public void closeGracefully() {
    if (isClosed) {
      return;
    }
    try {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } catch (Throwable throwable) {
      LOG.warn("Error while trying to close the append handle", throwable);
    }
    isClosed = true;
  }

  @Override
  public StoragePath getWritePath() {
    return writer.getLogFile().getPath();
  }

  protected HoodieLogFormat.Writer createLogWriter(String instantTime, String fileSuffix) {
    if (config.getWriteVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      throw new HoodieException("Flink RowData append handle do support Hudi table with version less than " + HoodieTableVersion.EIGHT.versionCode());
    }
    try {
      return HoodieLogFormat.newWriterBuilder()
          .onParentPath(FSUtils.constructAbsolutePath(hoodieTable.getMetaClient().getBasePath(), partitionPath))
          .withFileId(fileId)
          .withInstantTime(instantTime)
          .withFileSize(0L)
          .withSizeThreshold(config.getLogFileMaxSize())
          .withStorage(storage)
          .withLogWriteToken(writeToken)
          .withFileCreationCallback(getLogCreationCallback())
          .withTableVersion(config.getWriteVersion())
          .withSuffix(fileSuffix)
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
          .build();
    } catch (IOException e) {
      throw new HoodieException("Creating logger writer with fileId: " + fileId + ", "
          + "delta commit time: " + instantTime + ", "
          + "file suffix: " + fileSuffix + " error");
    }
  }

  /**
   * Returns a log creation hook impl.
   */
  protected LogFileCreationCallback getLogCreationCallback() {
    return new LogFileCreationCallback() {
      @Override
      public boolean preFileCreation(HoodieLogFile logFile) {
        WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
        return writeMarkers.createIfNotExists(partitionPath, logFile.getFileName(), IOType.CREATE,
            config, fileId, hoodieTable.getMetaClient().getActiveTimeline()).isPresent();
      }
    };
  }

  private HoodieLogBlock genDataBlock(
      HoodieWriteConfig writeConfig,
      HoodieLogBlock.HoodieLogBlockType logDataBlockFormat,
      List<HoodieRecord> records,
      String keyField,
      Map<HoodieLogBlock.HeaderMetadataType, String> header) {

    switch (logDataBlockFormat) {
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(
            records,
            header,
            keyField,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled());
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " is not implemented for Flink RowData append handle.");
    }
  }
}

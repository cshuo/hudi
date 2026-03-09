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

package org.apache.hudi.utilities;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.FileGroupReaderBasedMergeHandle;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Repairs a single MOR base file in-place without changing the table timeline.
 */
public class HoodieBaseFileRepairTool {

  private static final FixedTaskContextSupplier TASK_CONTEXT_SUPPLIER = new FixedTaskContextSupplier();

  private final Config cfg;

  public HoodieBaseFileRepairTool(Config cfg) {
    this.cfg = cfg;
  }

  public static void main(String[] args) throws Exception {
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    HoodieBaseFileRepairTool tool = new HoodieBaseFileRepairTool(cfg);
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("base-file-repair", cfg.sparkMaster, cfg.sparkMemory, cfg.enableHiveSupport);
    try {
      tool.run(jsc);
    } finally {
      jsc.stop();
    }
  }

  public void run(JavaSparkContext jsc) throws Exception {
    HoodieTableMetaClient metaClient = UtilHelpers.createMetaClient(jsc, cfg.basePath, true);
    ValidationUtils.checkArgument(metaClient.getTableType() == HoodieTableType.MERGE_ON_READ,
        "Base file repair only supports MOR tables");

    HoodieWriteConfig writeConfig = buildWriteConfig(jsc.hadoopConfiguration(), metaClient);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieSparkTable<IndexedRecord> table = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
    RepairPlan repairPlan = buildRepairPlan(engineContext, metaClient);

    if (cfg.validateOnly) {
      System.out.printf("Validated repair plan for fileId=%s, source=%s, target=%s%n",
          cfg.fileId, cfg.sourceBaseInstant, cfg.targetBaseInstant);
      return;
    }

    executeRepair(metaClient, table, writeConfig, repairPlan);
  }

  private HoodieWriteConfig buildWriteConfig(Configuration hadoopConf, HoodieTableMetaClient metaClient) throws Exception {
    HoodieSchema tableSchema = new TableSchemaResolver(metaClient).getTableSchema(false);
    return HoodieWriteConfig.newBuilder()
        .withPath(cfg.basePath)
        .forTable(metaClient.getTableConfig().getTableName())
        .withSchema(tableSchema.toString())
        .withProperties(UtilHelpers.buildProperties(hadoopConf, cfg.propsFilePath, cfg.configs))
        .build();
  }

  private RepairPlan buildRepairPlan(HoodieSparkEngineContext engineContext, HoodieTableMetaClient metaClient) {
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        engineContext, metaClient, metaClient.getCommitsAndCompactionTimeline());

    List<FileSlice> fileSlices = fsView.getAllFileSlices(cfg.partitionPath)
        .filter(slice -> cfg.fileId.equals(slice.getFileId()))
        .collect(Collectors.toList());

    FileSlice sourceSlice = getSlice(fileSlices, cfg.sourceBaseInstant, "source");
    FileSlice targetSlice = getSlice(fileSlices, cfg.targetBaseInstant, "target");

    ValidationUtils.checkArgument(sourceSlice.getFileGroupId().equals(targetSlice.getFileGroupId()),
        "Source and target slices must belong to the same file group");
    ValidationUtils.checkArgument(sourceSlice.getLogFiles().findAny().isPresent(),
        "Source slice must contain at least one log file");

    HoodieBaseFile targetBaseFile = targetSlice.getBaseFile().orElseThrow(() ->
        new HoodieException("Target slice does not have a base file"));
    ValidationUtils.checkArgument(cfg.targetBaseInstant.equals(targetBaseFile.getCommitTime()),
        "Target base file commit time does not match target base instant");

    List<org.apache.hudi.common.model.HoodieLogFile> sourceLogFiles = sourceSlice.getLogFiles()
        .sorted(org.apache.hudi.common.model.HoodieLogFile.getLogFileComparator())
        .collect(Collectors.toList());

    Map<String, Double> metrics = new HashMap<>();
    metrics.put(CompactionStrategy.TOTAL_LOG_FILE_SIZE,
        (double) sourceLogFiles.stream().mapToLong(org.apache.hudi.common.model.HoodieLogFile::getFileSize).sum());
    metrics.put(CompactionStrategy.TOTAL_LOG_FILES, (double) sourceLogFiles.size());

    CompactionOperation compactionOperation = new CompactionOperation(
        sourceSlice.getBaseFile(), cfg.partitionPath, sourceLogFiles, metrics);

    String targetFileName = targetBaseFile.getFileName();
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    String tempFileName = addSuffixBeforeExtension(targetFileName, ".repair-" + suffix);
    String backupFileName = addSuffixBeforeExtension(targetFileName, ".bak-" + suffix);

    StoragePath targetPath = targetBaseFile.getStoragePath();
    StoragePath tempPath = new StoragePath(targetPath.getParent(), tempFileName);
    StoragePath backupPath = new StoragePath(targetPath.getParent(), backupFileName);

    return new RepairPlan(compactionOperation, sourceSlice, targetSlice, targetPath, tempPath, backupPath);
  }

  private void executeRepair(HoodieTableMetaClient metaClient,
                             HoodieSparkTable<IndexedRecord> table,
                             HoodieWriteConfig writeConfig,
                             RepairPlan repairPlan) throws Exception {
    HoodieStorage storage = metaClient.getStorage();
    deleteIfExists(storage, repairPlan.tempPath);
    deleteIfExists(storage, repairPlan.backupPath);

    HoodieAvroReaderContext readerContext = new HoodieAvroReaderContext(
        metaClient.getStorageConf(), metaClient.getTableConfig(), Option.empty(), Option.empty(), writeConfig.getProps());

    FixedOutputFileGroupReaderBasedMergeHandle mergeHandle = FixedOutputFileGroupReaderBasedMergeHandle.create(
        writeConfig,
        cfg.targetBaseInstant,
        table,
        repairPlan.compactionOperation,
        TASK_CONTEXT_SUPPLIER,
        readerContext,
        cfg.targetBaseInstant,
        HoodieRecord.HoodieRecordType.AVRO,
        repairPlan.tempPath.getName());

    try {
      mergeHandle.doMerge();
      List<WriteStatus> writeStatuses = mergeHandle.close();
      ValidationUtils.checkArgument(!writeStatuses.isEmpty(), "Repair merge did not produce any write status");
      validateParquet(metaClient, repairPlan.tempPath);
      replaceTargetFile(storage, repairPlan.targetPath, repairPlan.tempPath, repairPlan.backupPath);
      validateParquet(metaClient, repairPlan.targetPath);
    } finally {
      WriteMarkersFactory.get(writeConfig.getMarkersType(), table, cfg.targetBaseInstant)
          .quietDeleteMarkerDir(table.getContext(), writeConfig.getMarkersDeleteParallelism());
      deleteIfExists(storage, repairPlan.tempPath);
      deleteIfExists(storage, repairPlan.backupPath);
    }
  }

  private void replaceTargetFile(HoodieStorage storage,
                                 StoragePath targetPath,
                                 StoragePath tempPath,
                                 StoragePath backupPath) throws IOException {
    boolean renamedTarget = false;
    try {
      ValidationUtils.checkArgument(storage.exists(targetPath), "Target file does not exist: " + targetPath);
      ValidationUtils.checkArgument(storage.exists(tempPath), "Temporary repair file does not exist: " + tempPath);
      ValidationUtils.checkArgument(storage.rename(targetPath, backupPath),
          "Failed to move target file to backup path");
      renamedTarget = true;
      ValidationUtils.checkArgument(storage.rename(tempPath, targetPath),
          "Failed to promote repaired file to target path");
      deleteIfExists(storage, backupPath);
    } catch (IOException | RuntimeException e) {
      if (renamedTarget && !storage.exists(targetPath) && storage.exists(backupPath)) {
        storage.rename(backupPath, targetPath);
      }
      throw e;
    }
  }

  private void validateParquet(HoodieTableMetaClient metaClient, StoragePath filePath) throws IOException {
    ParquetMetadata metadata = ParquetFileReader.readFooter(
        metaClient.getStorage().newInstance(filePath, metaClient.getStorageConf()).getConf().unwrapAs(Configuration.class),
        new Path(filePath.toUri()),
        ParquetMetadataConverter.NO_FILTER);
    ValidationUtils.checkArgument(metadata != null && metadata.getFileMetaData() != null,
        "Unable to read parquet footer from " + filePath);
  }

  private void deleteIfExists(HoodieStorage storage, StoragePath path) throws IOException {
    if (storage.exists(path)) {
      storage.deleteFile(path);
    }
  }

  private FileSlice getSlice(List<FileSlice> fileSlices, String baseInstant, String label) {
    return fileSlices.stream()
        .filter(slice -> baseInstant.equals(slice.getBaseInstantTime()))
        .max(Comparator.comparing(FileSlice::getBaseInstantTime))
        .orElseThrow(() -> new HoodieException("Unable to find " + label + " slice for instant " + baseInstant));
  }

  private static String addSuffixBeforeExtension(String fileName, String suffix) {
    int extensionIndex = fileName.lastIndexOf('.');
    ValidationUtils.checkArgument(extensionIndex > 0, "Expected file name with extension: " + fileName);
    return fileName.substring(0, extensionIndex) + suffix + fileName.substring(extensionIndex);
  }

  private static class RepairPlan {
    private final CompactionOperation compactionOperation;
    private final FileSlice sourceSlice;
    private final FileSlice targetSlice;
    private final StoragePath targetPath;
    private final StoragePath tempPath;
    private final StoragePath backupPath;

    private RepairPlan(CompactionOperation compactionOperation,
                       FileSlice sourceSlice,
                       FileSlice targetSlice,
                       StoragePath targetPath,
                       StoragePath tempPath,
                       StoragePath backupPath) {
      this.compactionOperation = compactionOperation;
      this.sourceSlice = sourceSlice;
      this.targetSlice = targetSlice;
      this.targetPath = targetPath;
      this.tempPath = tempPath;
      this.backupPath = backupPath;
    }
  }

  private static class FixedTaskContextSupplier extends TaskContextSupplier implements Serializable {
    private static final Supplier<Integer> ZERO_INT = () -> 0;
    private static final Supplier<Long> ZERO_LONG = () -> 0L;

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return ZERO_INT;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return ZERO_INT;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return ZERO_LONG;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return ZERO_INT;
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return ZERO_INT;
    }
  }

  private static class FixedOutputFileGroupReaderBasedMergeHandle extends FileGroupReaderBasedMergeHandle<IndexedRecord, Object, Object, Object> {
    private static final ThreadLocal<String> OUTPUT_FILE_NAME = new ThreadLocal<>();

    static FixedOutputFileGroupReaderBasedMergeHandle create(HoodieWriteConfig config,
                                                             String instantTime,
                                                             HoodieSparkTable<IndexedRecord> hoodieTable,
                                                             CompactionOperation compactionOperation,
                                                             TaskContextSupplier taskContextSupplier,
                                                             HoodieAvroReaderContext readerContext,
                                                             String maxInstantTime,
                                                             HoodieRecord.HoodieRecordType engineRecordType,
                                                             String outputFileName) {
      OUTPUT_FILE_NAME.set(outputFileName);
      try {
        return new FixedOutputFileGroupReaderBasedMergeHandle(
            config, instantTime, hoodieTable, compactionOperation, taskContextSupplier, readerContext, maxInstantTime, engineRecordType);
      } finally {
        OUTPUT_FILE_NAME.remove();
      }
    }

    FixedOutputFileGroupReaderBasedMergeHandle(HoodieWriteConfig config,
                                               String instantTime,
                                               HoodieTable hoodieTable,
                                               CompactionOperation compactionOperation,
                                               TaskContextSupplier taskContextSupplier,
                                               HoodieAvroReaderContext readerContext,
                                               String maxInstantTime,
                                               HoodieRecord.HoodieRecordType engineRecordType) {
      super(config, instantTime, hoodieTable, compactionOperation, taskContextSupplier, readerContext, maxInstantTime, engineRecordType);
    }

    @Override
    protected String createNewFileName(String oldFileName) {
      String outputFileName = OUTPUT_FILE_NAME.get();
      return outputFileName != null ? outputFileName : super.createNewFileName(oldFileName);
    }
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath;
    @Parameter(names = {"--partition-path", "-pp"}, description = "Partition path of the file group", required = true)
    public String partitionPath;
    @Parameter(names = {"--file-id", "-id"}, description = "File id of the target file group", required = true)
    public String fileId;
    @Parameter(names = {"--source-base-instant", "-si"}, description = "Base instant of the source slice", required = true)
    public String sourceBaseInstant;
    @Parameter(names = {"--target-base-instant", "-ti"}, description = "Base instant of the target base file to repair", required = true)
    public String targetBaseInstant;
    @Parameter(names = {"--validate-only", "-vo"}, description = "Validate without mutating files")
    public boolean validateOnly = false;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = "local[1]";
    @Parameter(names = {"--spark-memory", "-sm"}, description = "Spark executor memory", required = false)
    public String sparkMemory = "1G";
    @Parameter(names = {"--enable-hive-support", "-ehs"}, description = "Enable hive support")
    public Boolean enableHiveSupport = false;
    @Parameter(names = {"--props"}, description = "Path to properties file")
    public String propsFilePath = null;
    @Parameter(names = {"--hoodie-conf"}, description = "Additional hudi configs")
    public List<String> configs = java.util.Collections.emptyList();
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }
}

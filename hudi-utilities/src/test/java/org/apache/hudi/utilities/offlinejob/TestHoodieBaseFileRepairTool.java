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

package org.apache.hudi.utilities.offlinejob;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.utilities.HoodieBaseFileRepairTool;
import org.apache.hudi.utilities.HoodieCompactor;

import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieBaseFileRepairTool extends HoodieOfflineJobTestBase {

  @Test
  void testRepairCompactionBaseFileWithoutTimelineChange() throws Exception {
    String tableBasePath = basePath + "/repair-base-file";
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("repair_base_file")
        .withPath(tableBasePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false).withScheduleInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().logFileMaxSize(1024).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withAutoClean(false).withAsyncClean(false).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .build();
    props.putAll(config.getProps());

    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), tableBasePath);

    client = new SparkRDDWriteClient<>(context, config);
    writeData(true, 100, true);
    writeData(true, 100, true);

    HoodieCompactor compactorSchedule = initCompactor(tableBasePath, true, HoodieCompactor.SCHEDULE, true);
    compactorSchedule.compact(0);

    HoodieCompactor compactorExecute = initCompactor(tableBasePath, false, HoodieCompactor.EXECUTE, true);
    compactorExecute.compact(0);

    writeData(true, 100, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline completedTimeline = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants();
    int timelineCountBefore = completedTimeline.countInstants();

    String partitionPath = dataGen.getPartitionPaths()[0];
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, metaClient.getCommitsAndCompactionTimeline());
    List<FileSlice> slices = fsView.getAllFileSlices(partitionPath)
        .sorted(Comparator.comparing(FileSlice::getBaseInstantTime).reversed())
        .collect(Collectors.toList());

    assertTrue(slices.size() >= 2);
    FileSlice targetSlice = slices.get(0);
    FileSlice sourceSlice = slices.get(1);
    assertTrue(targetSlice.getBaseFile().isPresent());
    assertTrue(sourceSlice.getLogFiles().findAny().isPresent());

    long recordCount = sparkSession.read().format("hudi").load(tableBasePath).count();

    // simulate a corrupted file
    StoragePath corruptedPath = targetSlice.getBaseFile().get().getStoragePath();
    try (OutputStream outputStream = metaClient.getStorage().create(corruptedPath, true)) {
      outputStream.write(1);
    }

    HoodieBaseFileRepairTool.Config repairConfig = new HoodieBaseFileRepairTool.Config();
    repairConfig.basePath = tableBasePath;
    repairConfig.partitionPath = partitionPath;
    repairConfig.fileId = targetSlice.getFileId();
    repairConfig.sourceBaseInstant = sourceSlice.getBaseInstantTime();
    repairConfig.targetBaseInstant = targetSlice.getBaseInstantTime();
    repairConfig.sparkMaster = "local[1]";
    repairConfig.sparkMemory = "1G";

    new HoodieBaseFileRepairTool(repairConfig).run(jsc);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(timelineCountBefore, metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().countInstants());
    assertTrue(metaClient.getStorage().getPathInfo(corruptedPath).getLength() > 1);
    assertEquals(recordCount, sparkSession.read().format("hudi").load(tableBasePath).count());
  }

  private HoodieCompactor initCompactor(String tableBasePath, boolean runSchedule, String runningMode, boolean skipClean) {
    HoodieCompactor.Config config = new HoodieCompactor.Config();
    config.basePath = tableBasePath;
    config.runSchedule = runSchedule;
    config.runningMode = runningMode;
    config.configs.add("hoodie.metadata.enable=false");
    config.skipClean = skipClean;
    config.configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), 1));
    config.configs.add(String.format("%s=%s", HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), 1));
    return new HoodieCompactor(jsc, config);
  }
}

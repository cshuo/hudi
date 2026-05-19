/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A singleton source that periodically checks whether parquet files exist for rollback instants.
 */
public class RollbackFileMonitoringFunction extends RichSourceFunction<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(RollbackFileMonitoringFunction.class);

  private static final long serialVersionUID = 1L;

  private final Configuration conf;

  private final List<String> tablePaths;

  private final long checkIntervalSeconds;

  private volatile boolean running = true;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private transient Map<String, HoodieTableMetaClient> metaClients;

  private transient Set<String> reportedMatches;

  public RollbackFileMonitoringFunction(Configuration conf, List<String> tablePaths, long checkIntervalSeconds) {
    this.conf = conf;
    this.tablePaths = tablePaths;
    this.checkIntervalSeconds = checkIntervalSeconds;
  }

  @Override
  public void open(Configuration parameters) {
    this.hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    this.metaClients = new HashMap<>();
    this.reportedMatches = new HashSet<>();
  }

  @Override
  public void run(SourceContext<Object> context) throws Exception {
    while (running) {
      synchronized (context.getCheckpointLock()) {
        for (String tablePath : tablePaths) {
          scanTable(tablePath, context);
        }
      }
      TimeUnit.SECONDS.sleep(checkIntervalSeconds);
    }
  }

  private void scanTable(String tablePath, SourceContext<Object> context) throws IOException {
    if (!StreamerUtil.tableExists(tablePath, hadoopConf)) {
      LOG.warn("Skip rollback file monitoring because the Hudi table does not exist: {}", tablePath);
      return;
    }

    HoodieTableMetaClient metaClient = getOrReloadMetaClient(tablePath);
    Map<String, Set<String>> rolledBackInstants = getRolledBackInstants(metaClient);
    if (rolledBackInstants.isEmpty()) {
      LOG.warn("Rolled back instants is empty: {}", tablePath);
      return;
    } else {
      LOG.info("Rolled back instants: {}", rolledBackInstants);
    }

    FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
    String tableName = metaClient.getTableConfig().getTableName();
    FSUtils.processFiles(fs, tablePath, fileStatus -> {
      Path filePath = fileStatus.getPath();
      if (!isParquetFile(filePath)) {
        return true;
      }

      String fileInstant = tryGetInstant(filePath);
      if (fileInstant != null && rolledBackInstants.containsKey(fileInstant)) {
        reportMatch(tablePath, tableName, filePath, fileInstant, rolledBackInstants.get(fileInstant), fileStatus.getModificationTime());
      }
      return true;
    }, true);
  }

  private Map<String, Set<String>> getRolledBackInstants(HoodieTableMetaClient metaClient) throws IOException {
    Map<String, Set<String>> rolledBackInstants = new HashMap<>();
    List<HoodieInstant> completedRollbackInstants = metaClient.getActiveTimeline().getRollbackTimeline()
        .filterCompletedInstants().getInstantsAsStream().collect(Collectors.toList());
    for (HoodieInstant rollbackInstant : completedRollbackInstants) {
      if (!metaClient.getActiveTimeline().getInstantDetails(rollbackInstant).isPresent()) {
        LOG.warn("Skip rollback instant without metadata: {}", rollbackInstant);
        continue;
      }
      HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
          metaClient.getActiveTimeline().getInstantDetails(rollbackInstant).get());
      for (String rolledBackInstant : rollbackMetadata.getCommitsRollback()) {
        rolledBackInstants.computeIfAbsent(rolledBackInstant, ignored -> new HashSet<>())
            .add(rollbackInstant.getTimestamp());
      }
    }
    return rolledBackInstants;
  }

  private HoodieTableMetaClient getOrReloadMetaClient(String tablePath) {
    HoodieTableMetaClient metaClient = metaClients.get(tablePath);
    if (metaClient == null) {
      metaClient = StreamerUtil.createMetaClient(tablePath, hadoopConf);
    } else {
      metaClient = HoodieTableMetaClient.reload(metaClient);
    }
    metaClients.put(tablePath, metaClient);
    return metaClient;
  }

  private boolean isParquetFile(Path filePath) {
    return filePath.getName().endsWith(HoodieFileFormat.PARQUET.getFileExtension());
  }

  private String tryGetInstant(Path filePath) {
    try {
      return FSUtils.getCommitTime(filePath.getName());
    } catch (HoodieException e) {
      LOG.debug("Skip parquet file with non-Hudi base-file name: {}", filePath, e);
      return null;
    }
  }

  private void reportMatch(
      String tablePath,
      String tableName,
      Path filePath,
      String rolledBackInstant,
      Set<String> rollbackInstants,
      long modificationTime) {
    String matchKey = tablePath + "|" + filePath + "|" + rolledBackInstant;
    if (!reportedMatches.add(matchKey)) {
      return;
    }

    String tableIdentifier = StringUtils.isNullOrEmpty(tableName) ? tablePath : tableName;
    String message = String.format(
        "Found residual parquet file for rolled back instant. table=%s, fileName=%s, filePath=%s, rolledBackInstant=%s, rollbackInstants=%s, fileLastModified=%s, fileLastModifiedMs=%d",
        tableIdentifier, filePath.getName(), filePath, rolledBackInstant, rollbackInstants, Instant.ofEpochMilli(modificationTime), modificationTime);
    System.out.println(message);
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void close() throws Exception {
    running = false;
    super.close();
  }
}

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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

/**
 * todo add doc
 */
public abstract class FlinkWriteHandle<T, I, K, O> implements MiniBatchHandle, AutoCloseable {
  public final String instantTime;
  protected final HoodieWriteConfig config;
  protected final HoodieTable<T, I, K, O> hoodieTable;
  protected HoodieStorage storage;
  protected final String fileId;
  protected final String partitionPath;
  protected final String writeToken;
  protected final Schema writeSchema;
  protected final Schema writeSchemaWithMetaFields;
  protected final TaskContextSupplier taskContextSupplier;
  protected WriteStatus writeStatus;
  protected HoodieTimer timer;

  public FlinkWriteHandle(
      HoodieWriteConfig config,
      Option<String> instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String fileId,
      String partitionPath,
      TaskContextSupplier taskContextSupplier) {
    this.instantTime = instantTime.orElse(StringUtils.EMPTY_STRING);
    this.config = config;
    this.hoodieTable = hoodieTable;
    this.storage = hoodieTable.getStorage();
    this.fileId = fileId;
    this.partitionPath = partitionPath;
    this.taskContextSupplier = taskContextSupplier;
    this.writeSchema = getWriteSchema(config);
    this.writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        hoodieTable.shouldTrackSuccessRecords(), config.getWriteStatusFailureFraction());
    this.timer = HoodieTimer.start();
    this.writeToken = FSUtils.makeWriteToken(getPartitionId(), getStageId(), getAttemptId());
  }

  private static Schema getWriteSchema(HoodieWriteConfig config) {
    return new Schema.Parser().parse(config.getWriteSchema());
  }

  protected int getPartitionId() {
    return taskContextSupplier.getPartitionIdSupplier().get();
  }

  protected int getStageId() {
    return taskContextSupplier.getStageIdSupplier().get();
  }

  protected long getAttemptId() {
    return taskContextSupplier.getAttemptIdSupplier().get();
  }

  @Override
  public void close() throws Exception {
    this.closeGracefully();
  }
}

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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.ConfigUtils;
import org.apache.hudi.util.RowDataKeyGen;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

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
  protected final RowDataKeyGen rowDataKeyGen;
  protected final PreCombineFieldExtractor preCombineFieldExtractor;
  protected WriteStatus writeStatus;
  protected HoodieTimer timer;
  public static final int DEFAULT_ORDERING_VALUE = 0;

  public FlinkWriteHandle(
      HoodieWriteConfig config,
      RowType rowType,
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
    this.writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.populateMetaFields(), config.allowOperationMetadataField());
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        hoodieTable.shouldTrackSuccessRecords(), config.getWriteStatusFailureFraction());
    this.timer = HoodieTimer.start();
    this.writeToken = FSUtils.makeWriteToken(getPartitionId(), getStageId(), getAttemptId());
    this.rowDataKeyGen = RowDataKeyGen.instance(config.getProps(), rowType);
    this.preCombineFieldExtractor = getPreCombineFieldExtractor(config.getProps(), rowType);
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

  /**
   * Save hoodie partition meta in the partition path.
   */
  protected void initPartitionMeta() {
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
        new StoragePath(config.getBasePath()),
        FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
        hoodieTable.getPartitionMetafileFormat());
    partitionMetadata.trySave();
  }

  @Override
  public void close() throws Exception {
    this.closeGracefully();
  }

  /**
   * todo add doc
   *
   * @param props
   * @param rowType
   * @return
   */
  private static PreCombineFieldExtractor getPreCombineFieldExtractor(TypedProperties props, RowType rowType) {
    String preCombineField = ConfigUtils.getPreCombineField(props);
    if (StringUtils.isNullOrEmpty(preCombineField)) {
      // return a dummy extractor.
      return rowData -> DEFAULT_ORDERING_VALUE;
    }
    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();
    int preCombineFieldIdx = fieldNames.indexOf(preCombineField);
    RowData.FieldGetter preCombineFieldGetter = RowData.createFieldGetter(fieldTypes.get(preCombineFieldIdx), preCombineFieldIdx);

    return rowData -> {
      Object orderVal = preCombineFieldGetter.getFieldOrNull(rowData);
      if (orderVal instanceof TimestampData) {
        return ((TimestampData) orderVal).toInstant().toEpochMilli();
      } else {
        return (Comparable<?>) orderVal;
      }
    };
  }

  /**
   * todo add doc
   */
  public interface PreCombineFieldExtractor {
    Comparable<?> getPreCombineField(RowData rowData);
  }
}

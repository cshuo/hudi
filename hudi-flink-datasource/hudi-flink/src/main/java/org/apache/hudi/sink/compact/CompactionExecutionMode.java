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

package org.apache.hudi.sink.compact;

import org.apache.hudi.configuration.OptionsResolver;

import org.apache.flink.configuration.Configuration;

/**
 * The mode for the compaction execution of the compaction pipeline.
 */
public enum CompactionExecutionMode {
  /**
   * The mode for executing compaction for the data table only.
   */
  DATA_TABLE,

  /**
   * The mode for executing compaction for the metadata table only.
   */
  METADATA_TABLE,

  /**
   * The mode for executing compactions for both data table and metadata table.
   */
  ALL,

  /**
   * The mode for not executing compactions.
   */
  NONE;

  public static boolean isDataCompaction(CompactionExecutionMode compactionExecutionMode) {
    return compactionExecutionMode == DATA_TABLE || compactionExecutionMode == ALL;
  }

  public static boolean isMetadataCompaction(CompactionExecutionMode compactionExecutionMode) {
    return compactionExecutionMode == METADATA_TABLE || compactionExecutionMode == ALL;
  }

  public static CompactionExecutionMode fromConf(Configuration conf) {
    boolean compactionEnabled = OptionsResolver.needsAsyncCompaction(conf);
    boolean metadataCompactionEnabled = OptionsResolver.needsAsyncMetadataCompaction(conf);
    if (compactionEnabled && metadataCompactionEnabled) {
      return ALL;
    } else if (compactionEnabled) {
      return DATA_TABLE;
    } else if (metadataCompactionEnabled) {
      return METADATA_TABLE;
    } else {
      return NONE;
    }
  }
}

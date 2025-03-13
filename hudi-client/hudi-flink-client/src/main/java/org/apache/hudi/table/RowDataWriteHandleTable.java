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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.io.v2.RowDataWriteHandle;

import org.apache.flink.table.data.RowData;

import java.util.Iterator;
import java.util.List;

/**
 * HoodieTable that need to pass in the {@link RowDataWriteHandle} explicitly.
 */
public interface RowDataWriteHandleTable {
  /**
   * Upsert a batch of RowData into Hoodie table at the supplied instantTime.
   *
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> upsert(
      HoodieEngineContext context,
      RowDataWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);

  /**
   * Insert a batch of RowData into Hoodie table at the supplied instantTime.
   *
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> insert(
      HoodieEngineContext context,
      RowDataWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);

  /**
   * Replaces all the existing records and inserts the specified new RowDatas into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> insertOverwrite(
      HoodieEngineContext context,
      RowDataWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);

  /**
   * Deletes all the existing records of the Hoodie table and inserts the specified new RowDatas into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> insertOverwriteTable(
      HoodieEngineContext context,
      RowDataWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);
}

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.io.v2.FlinkWriteHandle;

import org.apache.flink.table.data.RowData;

import java.util.Iterator;
import java.util.List;

/**
 * todo add doc
 */
public interface RowDataWriteHandleTable {
  /**
   * todo
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> upsert(
      HoodieEngineContext context,
      FlinkWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);
  /**
   * todo
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> insert(
      HoodieEngineContext context,
      FlinkWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);
  /**
   * todo
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> insertOverwrite(
      HoodieEngineContext context,
      FlinkWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);
  /**
   * todo
   * @param context
   * @param writeHandle
   * @param instantTime
   * @param records
   * @return
   */
  List<WriteStatus> insertOverwriteTable(
      HoodieEngineContext context,
      FlinkWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      Iterator<RowData> records);
}

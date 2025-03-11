package org.apache.hudi.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Key generator for {@link RowData} that use an auto key generator.
 */
public class AutoRowDataKeyGen extends RowDataKeyGen {
  private final int taskId;
  private final String instantTime;
  private int rowId;

  public AutoRowDataKeyGen(
      int taskId,
      String instantTime,
      String partitionFields,
      RowType rowType,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath) {
    super(Option.empty(), partitionFields, rowType,
        hiveStylePartitioning, encodePartitionPath, false, Option.empty());
    this.taskId = taskId;
    this.instantTime = instantTime;
  }

  public static RowDataKeyGen instance(TypedProperties props, RowType rowType, int taskId, String instantTime) {
    return new AutoRowDataKeyGen(
        taskId,
        instantTime,
        props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()),
        rowType,
        props.getBoolean(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key()),
        props.getBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key()));
  }

  @Override
  public String getRecordKey(RowData rowData) {
    return HoodieRecord.generateSequenceId(instantTime, taskId, rowId++);
  }
}

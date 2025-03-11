package org.apache.hudi.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * Factory class for all kinds of {@link RowDataKeyGen}.
 */
public class RowDataKeyGens {
  /**
   * Creates a {@link RowDataKeyGen} with given configuration.
   */
  public static RowDataKeyGen instance(
      TypedProperties properties,
      RowType rowType,
      int taskId, String instantTime) {
    String recordKeys = properties.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key());
    if (hasRecordKey(recordKeys, rowType.getFieldNames())) {
      return RowDataKeyGen.instance(properties, rowType);
    } else {
      return AutoRowDataKeyGen.instance(properties, rowType, taskId, instantTime);
    }
  }

  /**
   * Checks whether user provides any record key.
   */
  private static boolean hasRecordKey(String recordKeys, List<String> fieldNames) {
    return recordKeys.split(",").length != 1
        || fieldNames.contains(recordKeys);
  }
}

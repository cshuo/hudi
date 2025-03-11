package org.apache.hudi.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.flink.table.types.logical.RowType;

/**
 * todo add doc
 */
public class ConfigUtils {

  public static final String NO_PRE_COMBINE = "no_precombine";

  /**
   * todo add doc
   * @param props
   * @return
   */
  public static String getPreCombineField(TypedProperties props) {
    String preCombineField = props.getString("precombine.field", null);
    if (preCombineField != null && !preCombineField.equalsIgnoreCase(NO_PRE_COMBINE)) {
      return preCombineField;
    }
    preCombineField = props.getString("write.precombine.field", null);
    if (preCombineField != null && !preCombineField.equalsIgnoreCase(NO_PRE_COMBINE)) {
      return preCombineField;
    }
    preCombineField = props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), null);
    if (preCombineField != null && !preCombineField.equalsIgnoreCase(NO_PRE_COMBINE)) {
      return preCombineField;
    } else {
      return null;
    }
  }
}

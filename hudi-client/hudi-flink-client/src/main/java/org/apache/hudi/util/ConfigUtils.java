package org.apache.hudi.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;

/**
 * todo add doc
 */
public class ConfigUtils {
  /**
   * todo add doc
   * @param props
   * @return
   */
  public static String getPreCombineField(TypedProperties props) {
    String preCombineField = props.getString("precombine.field", null);
    if (preCombineField != null) {
      return preCombineField;
    }
    preCombineField = props.getString("write.precombine.field", null);
    if (preCombineField != null) {
      return preCombineField;
    }
    preCombineField = props.getString(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), null);
    return preCombineField;
  }
}

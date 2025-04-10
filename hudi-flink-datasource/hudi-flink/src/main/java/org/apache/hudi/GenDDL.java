package org.apache.hudi;

public class GenDDL {
  public static void main(String[] args) {
    StringBuilder srcDDL = new StringBuilder();
    int numStringFields = 500;
    srcDDL.append("CREATE TABLE item_uv_pv_1d_source (\n");
    srcDDL.append("   `item_id` BIGINT,\n");
    for (int i = 0; i < numStringFields - 1; i++) {
      srcDDL.append(String.format("   `f_%d` STRING,\n", i));
    }
    srcDDL.append(String.format("   `f_%d` STRING)\n", numStringFields - 1));
    srcDDL.append("WITH (\n" +
        "    'connector' = 'datagen',\n" +
        "    'rows-per-second' = '999999999',\n" +
        "    'fields.item_id.min' = '0',\n" +
        "    'fields.item_id.max' = '99999999')");
    System.out.println(srcDDL.toString());
  }
}

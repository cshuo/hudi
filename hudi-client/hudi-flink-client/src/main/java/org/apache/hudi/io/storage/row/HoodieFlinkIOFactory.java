package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.hadoop.HoodieHadoopIOFactory;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;

/**
 * todo
 */
public class HoodieFlinkIOFactory extends HoodieHadoopIOFactory {
  public HoodieFlinkIOFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.FLINK) {
      return new HoodieRowDataFileWriterFactory(storage);
    }
    return super.getWriterFactory(recordType);
  }
}

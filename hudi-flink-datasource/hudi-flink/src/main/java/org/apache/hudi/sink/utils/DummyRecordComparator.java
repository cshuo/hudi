package org.apache.hudi.sink.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.RecordComparator;

/**
 * A dummy implementation of @{code RecordComparator} for pkless table.
 */
public class DummyRecordComparator implements RecordComparator {
  @Override
  public int compare(RowData rowData, RowData rowData1) {
    return 0;
  }
}

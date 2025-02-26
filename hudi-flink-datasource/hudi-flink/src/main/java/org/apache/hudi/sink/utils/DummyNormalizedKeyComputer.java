package org.apache.hudi.sink.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.operators.sort.SortUtil;

/**
 * A dummy implementation of @{code NormalizedKeyComputer} for pkless table.
 */
public class DummyNormalizedKeyComputer implements NormalizedKeyComputer {
  @Override
  public void putKey(RowData rowData, MemorySegment target, int offset) {
    SortUtil.minNormalizedKey(target, offset, 1);
  }

  @Override
  public int compareKey(MemorySegment memorySegment, int i, MemorySegment target, int offset) {
    return 0;
  }

  @Override
  public void swapKey(MemorySegment seg1, int index1, MemorySegment seg2, int index2) {
    // do nothing.
  }

  @Override
  public int getNumKeyBytes() {
    return 1;
  }

  @Override
  public boolean isKeyFullyDetermines() {
    return true;
  }

  @Override
  public boolean invertKey() {
    return false;
  }
}

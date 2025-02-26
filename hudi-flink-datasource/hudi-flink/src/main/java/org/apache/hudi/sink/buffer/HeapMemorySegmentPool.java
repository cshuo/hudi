package org.apache.hudi.sink.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.util.LinkedList;
import java.util.List;

/**
 * A heap memory based ${code MemorySegmentPool}.
 */
public class HeapMemorySegmentPool implements MemorySegmentPool {

  private final LinkedList<MemorySegment> cachePages;
  private final int maxPages;
  private final int pageSize;
  private int allocateNum;

  public HeapMemorySegmentPool(int pageSize, long maxSizeInBytes) {
    this.cachePages = new LinkedList<>();
    this.maxPages = (int) (maxSizeInBytes / pageSize);
    this.pageSize = pageSize;
    this.allocateNum = 0;
  }

  @Override
  public int pageSize() {
    return this.pageSize;
  }

  @Override
  public void returnAll(List<MemorySegment> list) {
    cachePages.addAll(list);
  }

  @Override
  public int freePages() {
    return maxPages - allocateNum + cachePages.size();
  }

  @Override
  public MemorySegment nextSegment() {
    if (!cachePages.isEmpty()) {
      return cachePages.poll();
    }
    if (freePages() > 0) {
      allocateNum += 1;
      return MemorySegmentFactory.wrap(new byte[pageSize]);
    }
    return null;
  }
}

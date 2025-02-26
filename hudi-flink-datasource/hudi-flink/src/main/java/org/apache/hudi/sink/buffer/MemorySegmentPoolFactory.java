package org.apache.hudi.sink.buffer;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

/**
 * todo
 */
public class MemorySegmentPoolFactory {
  // currrently support heap pool only, todo support pool based on flink managed memory
  public static MemorySegmentPool createMemorySegmentPool(Configuration conf) {
    long mergeReaderMem = 100; // constant 100MB
    long mergeMapMaxMem = conf.get(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
    long maxBufferSize = (long) ((conf.get(FlinkOptions.WRITE_TASK_MAX_SIZE) - mergeReaderMem - mergeMapMaxMem) * 1024 * 1024);
    final String errMsg = String.format("'%s' should be at least greater than '%s' plus merge reader memory(constant 100MB now)",
        FlinkOptions.WRITE_TASK_MAX_SIZE.key(), FlinkOptions.WRITE_MERGE_MAX_MEMORY.key());
    ValidationUtils.checkState(maxBufferSize > 0, errMsg);

    return new HeapMemorySegmentPool(conf.get(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE), maxBufferSize);
  }
}

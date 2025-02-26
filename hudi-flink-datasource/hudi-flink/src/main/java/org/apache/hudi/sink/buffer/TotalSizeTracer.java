package org.apache.hudi.sink.buffer;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;

/**
 * Tool to trace the total buffer size. It computes the maximum buffer size,
 * if current buffer size is greater than the maximum buffer size, the data bucket
 * flush triggers.
 */
public class TotalSizeTracer {
  public long bufferSize = 0L;
  public final double maxBufferSize;

  public TotalSizeTracer(Configuration conf) {
    long mergeReaderMem = 100; // constant 100MB
    long mergeMapMaxMem = conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
    this.maxBufferSize = (conf.getDouble(FlinkOptions.WRITE_TASK_MAX_SIZE) - mergeReaderMem - mergeMapMaxMem) * 1024 * 1024;
    final String errMsg = String.format("'%s' should be at least greater than '%s' plus merge reader memory(constant 100MB now)",
        FlinkOptions.WRITE_TASK_MAX_SIZE.key(), FlinkOptions.WRITE_MERGE_MAX_MEMORY.key());
    ValidationUtils.checkState(this.maxBufferSize > 0, errMsg);
  }

  /**
   * Trace the given record size {@code recordSize}.
   *
   * @param recordSize The record size
   * @return true if the buffer size exceeds the maximum buffer size
   */
  public boolean trace(long recordSize) {
    this.bufferSize += recordSize;
    return this.bufferSize > this.maxBufferSize;
  }

  public void countDown(long size) {
    this.bufferSize -= size;
  }

  public void reset() {
    this.bufferSize = 0;
  }
}

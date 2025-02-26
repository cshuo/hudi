package org.apache.hudi.util;

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Caution: call hasNext() before next().
 */
public class MutableIteratorWrapper<T, R> implements Iterator<R> {
  private final MutableObjectIterator<T> innerItr;
  private final Supplier<T> rowSupplier;
  private R curRow;
  private final Converter<T, R> converter;
  private final String instantTime;

  public MutableIteratorWrapper(
      MutableObjectIterator<T> innerItr,
      String instantTime,
      Supplier<T> rowSupplier,
      Converter<T, R> converter) {
    this.innerItr = innerItr;
    this.instantTime = instantTime;
    this.rowSupplier = rowSupplier;
    this.converter = converter;
  }

  @Override
  public boolean hasNext() {
    if (curRow != null) {
      return true;
    }
    try {
      T t = innerItr.next(rowSupplier.get());
      if (t == null) {
        return false;
      }
      this.curRow = converter.convert(t, instantTime);
    } catch (IOException e) {
      throw new HoodieException("Failed to get next record from inner iterator.", e);
    }
    return true;
  }

  @Override
  public R next() {
    R result = curRow;
    curRow = null;
    return result;
  }

  public interface Converter<T, R> {
    R convert(T t, String instantTime);
  }
}

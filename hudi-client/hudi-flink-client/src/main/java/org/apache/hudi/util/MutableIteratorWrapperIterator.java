package org.apache.hudi.util;

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

public class MutableIteratorWrapperIterator<T> implements Iterator<T> {
  private final MutableObjectIterator<T> innerItr;
  private final Supplier<T> rowSupplier;
  private T curRow;

  public MutableIteratorWrapperIterator(
      MutableObjectIterator<T> innerItr,
      Supplier<T> rowSupplier) {
    this.innerItr = innerItr;
    this.rowSupplier = rowSupplier;
  }

  @Override
  public boolean hasNext() {
    if (curRow != null) {
      return true;
    }
    try {
      curRow = innerItr.next(rowSupplier.get());
      return curRow != null;
    } catch (IOException e) {
      throw new HoodieException("Failed to get next record from inner iterator.", e);
    }
  }

  @Override
  public T next() {
    T result = curRow;
    curRow = null;
    return result;
  }
}

package org.apache.hudi.util;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * todo add doc
 */
public class IteratorWrapperList<E> implements List<E> {
  private final Iterator<E> innerItr;

  public IteratorWrapperList(Iterator<E> innerItr) {
    this.innerItr = innerItr;
  }

  @Override
  public @NotNull Iterator<E> iterator() {
    return this.innerItr;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public @NotNull Object[] toArray() {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public @NotNull <T> T[] toArray(@NotNull T[] a) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends E> c) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean addAll(int index, @NotNull Collection<? extends E> c) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void clear() {
    // do nothing.
  }

  @Override
  public E get(int index) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public E set(int index, E element) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void add(int index, E element) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public E remove(int index) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public @NotNull ListIterator<E> listIterator() {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public @NotNull ListIterator<E> listIterator(int index) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public @NotNull List<E> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException("operation not supported");
  }
}

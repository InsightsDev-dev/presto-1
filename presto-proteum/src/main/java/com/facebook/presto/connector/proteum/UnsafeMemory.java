package com.facebook.presto.connector.proteum;


import java.lang.reflect.Field;

import sun.misc.Unsafe;

public class UnsafeMemory {
  private static final Unsafe unsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);

  private static final int SIZE_OF_BOOLEAN = 1;
  private static final int SIZE_OF_BYTE = 1;
  private static final int SIZE_OF_INT = 4;
  private static final int SIZE_OF_LONG = 8;
  private static final int SIZE_OF_DOUBLE = 8;

  private final double RESIZE_THRESHOLD = 0.75;

  private int pos = 0;
  private byte[] buffer;
  private int capacity = 0;

  public UnsafeMemory(final byte[] buffer) {
    if (null == buffer) {
      throw new NullPointerException("buffer cannot be null");
    }

    this.buffer = buffer;
    this.capacity = buffer.length;
  }

  public boolean hasNext() {
    return pos < capacity;
  }

  public int getPosition() {
    return this.pos;
  }

  public byte[] getBuffer() {
    return this.buffer;
  }

  public void reset() {
    this.pos = 0;
  }

  public void putBoolean(final boolean value) {
    if (pos + SIZE_OF_BOOLEAN > capacity * RESIZE_THRESHOLD) {
      resize();
    }
    unsafe.putBoolean(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_BOOLEAN;
  }

  public boolean getBoolean() {
    boolean value = unsafe.getBoolean(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_BOOLEAN;

    return value;
  }

  public void putString(final String value) {
    byte[] vals = value.getBytes();
    if ((pos + SIZE_OF_INT + vals.length * SIZE_OF_BYTE) > capacity * RESIZE_THRESHOLD) {
      resize();
    }
    putInt(vals.length);
    unsafe.copyMemory(vals, byteArrayOffset, buffer, byteArrayOffset + pos, vals.length);
    pos += vals.length * SIZE_OF_BYTE;
  }

  public String getString() {
    int length = getInt();
    byte[] vals = new byte[length];
    unsafe.copyMemory(buffer, byteArrayOffset + pos, vals, byteArrayOffset, length);
    pos += vals.length * SIZE_OF_BYTE;
    return new String(vals);
  }

  public void putInt(final int value) {
    if (pos + SIZE_OF_INT > capacity * RESIZE_THRESHOLD) {
      resize();
    }
    unsafe.putInt(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_INT;
  }

  public int getInt() {
    int value = unsafe.getInt(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_INT;

    return value;
  }

  public void putLong(final long value) {
    if (pos + SIZE_OF_LONG > capacity * RESIZE_THRESHOLD) {
      resize();
    }
    unsafe.putLong(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_LONG;
  }

  public long getLong() {
    long value = unsafe.getLong(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_LONG;

    return value;
  }

  public void putDouble(final double value) {
    if (pos + SIZE_OF_DOUBLE > capacity * RESIZE_THRESHOLD) {
      resize();
    }
    unsafe.putDouble(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_DOUBLE;
  }

  public double getDouble() {
    double value = unsafe.getDouble(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_DOUBLE;

    return value;
  }

  public void resize() {
    int newsize = capacity * 2;
    byte[] newBuffer = new byte[newsize];
    unsafe.copyMemory(buffer, byteArrayOffset, newBuffer, byteArrayOffset, pos);
    this.buffer = newBuffer;
    this.capacity = newsize;
  }
}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class OffHeapIndexInputStream extends IndexInput implements Cloneable {

  private final com.terracottatech.offheapstore.filesystem.File                file;
  private final com.terracottatech.offheapstore.filesystem.SeekableInputStream stream;

  public OffHeapIndexInputStream(com.terracottatech.offheapstore.filesystem.File file) throws IOException {
    super(file.getName());
    this.file = file;
    stream = file.getInputStream();
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) stream.read();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    stream.readBytes(b, offset, len);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public synchronized long getFilePointer() {
    long pointer = 0;
    try {
      pointer = stream.getFilePointer();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return pointer;
  }

  @Override
  public void seek(long pos) throws IOException {
    stream.seek(pos);
  }

  @Override
  public synchronized long length() {
    long length = 0;
    try {
      length = file.length();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return length;
  }

  @Override
  public synchronized OffHeapIndexInputStream clone() {
    OffHeapIndexInputStream copy;
    try {
      copy = new OffHeapIndexInputStream(file);
      copy.seek(getFilePointer());
      return copy;
    } catch (IOException e) {
      throw new AssertionError(e);
    }

  }

}

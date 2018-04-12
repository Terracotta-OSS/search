/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.store;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class OffHeapIndexOutputStream extends IndexOutput {

  private final com.terracottatech.offheapstore.filesystem.SeekableOutputStream stream;

  public OffHeapIndexOutputStream(com.terracottatech.offheapstore.filesystem.File file) throws IOException {
    stream = file.getOutputStream();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    stream.write(b & 0xff);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    stream.write(b, offset, length);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public long getFilePointer() {
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
  public long length() throws IOException {
    return stream.length();
  }
}

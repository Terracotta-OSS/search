/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ThreadInterruptedException;

import com.terracottatech.offheapstore.filesystem.FileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class OffHeapDirectory extends Directory implements Serializable {

  private final com.terracottatech.offheapstore.filesystem.Directory directory;

  public OffHeapDirectory(FileSystem fileSystem, String name) throws IOException {
    if (fileSystem.directoryExists(name)) {
      fileSystem.deleteDirectory(name);
    }
    directory = fileSystem.getOrCreateDirectory(name);
    setLockFactory(new ConcurrentLockFactory());
  }

  @Override
  public synchronized String[] listAll() throws IOException {
    Set<String> filesSet = directory.listFiles();
    List<String> names = new ArrayList<String>(filesSet.size());
    for (String fname : filesSet) {
      names.add(fname);
    }
    return names.toArray(new String[names.size()]);
  }

  @Override
  public boolean fileExists(String name) {
    return directory.fileExists(name);
  }

  @Override
  public long fileModified(String name) throws IOException {
    return directory.getOrCreateFile(name).lastModifiedTime();
  }

  /**
   * Set the modified time of an existing file to now.
   * 
   * @throws IOException if the file does not exist
   * @deprecated Lucene never uses this API; it will be removed in 4.0.
   */
  @Override
  @Deprecated
  public void touchFile(String name) throws IOException {
    if (!fileExists(name)) throw new FileNotFoundException(name);
    long ts2, ts1 = System.currentTimeMillis();
    do {
      try {
        Thread.sleep(0, 1);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
      ts2 = System.currentTimeMillis();
    } while (ts1 == ts2);
    directory.getOrCreateFile(name).setLastModifiedTime(ts2);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    if (!fileExists(name)) throw new FileNotFoundException(name);
    directory.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    return directory.getOrCreateFile(name).length();
  }

  /**
   * Creates a new, empty file in the directory with the given name. Returns a stream writing this file.
   * 
   * @throws IOException
   */
  @Override
  public IndexOutput createOutput(String name) throws IOException {
    return new OffHeapIndexOutputStream(directory.getOrCreateFile(name));
  }

  /** Returns a stream reading an existing file. */
  @Override
  public IndexInput openInput(String name) throws IOException {
    return new OffHeapIndexInputStream(directory.getOrCreateFile(name));
  }

  /** Closes the store to future operations, releasing associated memory. */
  @Override
  public synchronized void close() throws IOException {
    directory.deleteAllFiles();
    isOpen = false;
  }

}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.store;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.terracottatech.offheapstore.filesystem.File;
import com.terracottatech.offheapstore.filesystem.FileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public class OffHeapDirectory extends BaseDirectory implements Serializable {

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
    return directory.listFiles().toArray(new String[] {});
  }

  @Override
  public boolean fileExists(String name) {
    return directory.fileExists(name);
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
  public IndexOutput createOutput(String name, IOContext ctxt) throws IOException {
    return new OffHeapIndexOutputStream(directory.getOrCreateFile(name));
  }

  /** Returns a stream reading an existing file. */
  @Override
  public IndexInput openInput(String name, IOContext ctxt) throws IOException {
    return new OffHeapIndexInputStream(directory.getOrCreateFile(name));
  }

  /** Closes the store to future operations, releasing associated memory. */
  @Override
  public synchronized void close() throws IOException {
    directory.deleteAllFiles();
    isOpen = false;
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    Set<String> knownFiles = directory.listFiles();
    for (String fileName : names) {
      if (!knownFiles.contains(fileName)) {
        // XXX warn
        continue;
      }
      File f = directory.getOrCreateFile(fileName);
      f.getOutputStream().flush();
    }

  }

}

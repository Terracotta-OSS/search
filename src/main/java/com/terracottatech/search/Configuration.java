/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogMergePolicy;

public class Configuration {

  public static final float DEFAULT_MAX_BUFFER_SIZE = 16.0F;

  private float             maxRamBufferSize        = DEFAULT_MAX_BUFFER_SIZE;
  private int               maxBufferedDocs         = IndexWriter.DEFAULT_MAX_BUFFERED_DOCS;
  private int               mergeFactor             = LogMergePolicy.DEFAULT_MERGE_FACTOR;
  private int               maxMergeDocs            = LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;

  private final int         indexesPerCache;
  private final int         maxConcurrentQueries;
  private final boolean     useOffHeap;
  private final boolean     useRamDir;
  private final boolean     useCommitThread;
  private final int         offHeapFileSegmentCount;
  private final int         offHeapFileBlockSize;
  private final int         offHeapFileMaxPageSize;

  private boolean           doAccessChecks          = true;

  public Configuration(int indexesPerCache, int maxConcurrentQueries, boolean useOffHeap, boolean useRamDir,
                       boolean useCommitThread, int offHeapFileSegmentCount, int offHeapFileBlockSize,
                       int offHeapFileMaxPageSize) {
    this.indexesPerCache = indexesPerCache;
    this.maxConcurrentQueries = maxConcurrentQueries;
    this.useOffHeap = useOffHeap;
    this.useRamDir = useRamDir;
    this.useCommitThread = useCommitThread;
    this.offHeapFileSegmentCount = offHeapFileSegmentCount;
    this.offHeapFileBlockSize = offHeapFileBlockSize;
    this.offHeapFileMaxPageSize = offHeapFileMaxPageSize;
  }

  public boolean useRamDir() {
    return useRamDir;
  }

  public boolean useOffHeap() {
    return useOffHeap;
  }

  public int maxConcurrentQueries() {
    return maxConcurrentQueries;
  }

  public int indexesPerCache() {
    return indexesPerCache;
  }

  public boolean useCommitThread() {
    return useCommitThread;
  }

  public int getOffHeapFileSegmentCount() {
    return offHeapFileSegmentCount;
  }

  public int getOffHeapFileBlockSize() {
    return offHeapFileBlockSize;
  }

  public int getOffHeapFileMaxPageSize() {
    return offHeapFileMaxPageSize;
  }

  public float getMaxRamBufferSize() {
    return maxRamBufferSize;
  }

  public int getMergeFactor() {
    return mergeFactor;
  }

  public int getMaxBufferedDocs() {
    return maxBufferedDocs;
  }

  public int getMaxMergeDocs() {
    return maxMergeDocs;
  }

  public void setMaxRamBufferSize(float maxRamBufferSize) {
    this.maxRamBufferSize = maxRamBufferSize;
  }

  public void setMaxBufferedDocs(int maxBufferedDocs) {
    this.maxBufferedDocs = maxBufferedDocs;
  }

  public void setMergeFactor(int mergeFactor) {
    this.mergeFactor = mergeFactor;
  }

  public void setMaxMergeDocs(int maxMergeDocs) {
    this.maxMergeDocs = maxMergeDocs;
  }

  public boolean doAccessChecks() {
    return this.doAccessChecks;
  }

  public void setDoAccessChecks(boolean doChecks) {
    this.doAccessChecks = doChecks;
  }

}

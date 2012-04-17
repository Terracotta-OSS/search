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

  public boolean useRamDir() {
    throw new AssertionError();
  }

  public boolean useOffHeap() {
    throw new AssertionError();
  }

  public int maxConcurrentQueries() {
    throw new AssertionError();
  }

  public int indexesPerCahce() {
    throw new AssertionError();
  }

  public boolean useCommitThread() {
    throw new AssertionError();
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

}

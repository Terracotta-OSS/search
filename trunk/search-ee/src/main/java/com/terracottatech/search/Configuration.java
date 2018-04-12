/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.search.BooleanQuery;

public class Configuration {

  private double        maxRamBufferSize = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
  private int           maxBufferedDocs  = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;
  private int           mergeFactor      = LogMergePolicy.DEFAULT_MERGE_FACTOR;
  private int           maxMergeDocs     = LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;
  
  /**
   * Max number of merge threads allowed to be running at once. When there are more merges then this, we forcefully
   * pause the larger ones, letting the smaller ones run, up until maxMergeCount merges at which point we forcefully
   * pause incoming threads (that presumably are the ones causing so much merging). We dynamically default this from 1
   * to 8, depending on how many cores you have.
   * <p>
   * Defaults for this field and {@link #getMaxMergeCount()} have been copied here from {@link ConcurrentMergeScheduler}
   * in Lucene 4.0, with a change to upper limit on maxMergeThreadCount to 8. The reason for redefining the defaults
   * here is that these limits were lowered in Lucene 4.1 as part of <a
   * href="https://issues.apache.org/jira/browse/LUCENE-4661">LUCENE-4661</a>, hurting indexing performance on our most
   * commonly tested storage hardware (SSD). We do allow overriding maxMergeThreadCount defaults externally.
   * 
   * @see {@link ConcurrentMergeScheduler#getMaxThreadCount()},
   */
  private int maxMergeThreadCount = Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors() / 2));

  private boolean           doAccessChecks   = true;

  private final int         indexesPerCache;
  private final int         maxConcurrentQueries;
  private final boolean     useOffHeap;
  private final boolean     useRamDir;
  private final boolean     useCommitThread;
  private final int         offHeapFileSegmentCount;
  private final int         offHeapFileBlockSize;
  private final int         offHeapFileMaxPageSize;
  private boolean           disableStoredFieldCompression = false;
  private int               maxResultBatchSize            = 10000;
  private int               maxOpenResultSets             = 1000;
  private boolean           spoolSearchResults = false;

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

  public double getMaxRamBufferSize() {
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

  public int getMaxClauseCount() {
    return BooleanQuery.getMaxClauseCount();
  }

  /**
   * See {@link ConcurrentMergeScheduler#setMaxThreadCount(int)}
   */
  public int getMaxMergeThreadCount() {
    return maxMergeThreadCount;
  }
  
  /**
   * See {@link ConcurrentMergeScheduler#setMaxMergeCount(int)}
   */
  public int getMaxMergeCount() {
    return maxMergeThreadCount + 2;
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

  public void setMaxMergeThreadCount(int max) {
    this.maxMergeThreadCount = max;
  }
  
  public void setMaxClauseCount(int max) {
    BooleanQuery.setMaxClauseCount(max);
  }

  public boolean disableStoredFieldCompression() {
    return this.disableStoredFieldCompression;
  }

  public void setDisableStoredFieldCompression(boolean choice) {
    this.disableStoredFieldCompression = choice;
  }

  public int getMaxResultBatchSize() {
    return maxResultBatchSize;
  }

  public void setMaxResultBatchSize(int maxReplyPrefetchSize) {
    this.maxResultBatchSize = maxReplyPrefetchSize;
  }

  public int getMaxOpenResultSets() {
    return maxOpenResultSets;
  }

  public void setMaxOpenResultSets(int maxOpenResultSets) {
    this.maxOpenResultSets = maxOpenResultSets;
  }

  public boolean isSpoolSearchResults() {
    return spoolSearchResults;
  }

  public void setSpoolSearchResults(boolean choice) {
    this.spoolSearchResults = choice;
  }
}

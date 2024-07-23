/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

final class IndexFileImpl implements IndexFile {

  private final String  destFilename;
  private final String  luceneFilename;
  private final String  indexId;
  private final boolean isTCFile;
  private final long    length;

  public IndexFileImpl(String destFilename, String luceneFilename, String indexId, boolean isTCFile, long length) {
    this.destFilename = destFilename;
    this.luceneFilename = luceneFilename;
    this.indexId = indexId;
    this.isTCFile = isTCFile;
    this.length = length;
  }

  public boolean isTCFile() {
    return isTCFile;
  }

  public String getDestFilename() {
    return destFilename;
  }

  public String getLuceneFilename() {
    return luceneFilename;
  }

  public long length() {
    return length;
  }

  public String getIndexId() {
    return indexId;
  }
}

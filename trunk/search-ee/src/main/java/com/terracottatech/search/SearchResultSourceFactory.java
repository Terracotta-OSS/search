/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

class SearchResultSourceFactory {

  private final Configuration cfg;
  private final File          indexDataPath;
  private IndexOwner          owner;
  private LoggerFactory       logFactory;

  private final static String subDirName = "search-results";

  SearchResultSourceFactory(Configuration config, File dataPath) {
    cfg = config;
    indexDataPath = dataPath;
  }

  SearchResultSourceFactory setIndexOwner(IndexOwner parent) {
    owner = parent;
    return this;
  }

  SearchResultSourceFactory setLoggerFactory(LoggerFactory logFactory) {
    this.logFactory = logFactory;
    return this;
  }

  SearchResultSource createSource(IndexReader reader, QueryInputs query, int totalSize) throws IndexException, IOException {
    if (cfg.isSpoolSearchResults()) {
      File resPath = new File(indexDataPath, subDirName);
      Configuration conf = new Configuration(1, Integer.MAX_VALUE, false, false, false, -1, -1, -1);
      conf.setDisableStoredFieldCompression(true);
      conf.setDoAccessChecks(false);
      @SuppressWarnings({"unused" })
      LuceneIndex resultsIndex = new LuceneIndex(FSDirectory.open(resPath), subDirName, resPath, owner, conf,
                                                 logFactory);
      throw new IllegalArgumentException("Not implemented yet.");       // XXX use materialized results source

    }
    return new CachedDocIdResultSource(reader, query, totalSize);
  }


}

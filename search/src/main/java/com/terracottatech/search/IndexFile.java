/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

public interface IndexFile {

  boolean isTCFile();

  String getDestFilename();

  String getLuceneFilename();

  long length();

  String getIndexId(); // striped idx id, as in <lucene_dir>/<cache_name>/<index_id>/[lucene index files...]
}

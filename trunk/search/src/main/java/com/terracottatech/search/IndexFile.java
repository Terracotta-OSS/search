/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

public interface IndexFile {

  boolean isTCFile();

  String getDestFilename();

  String getLuceneFilename();

  long length();

  String getIndexId(); // striped idx id, as in <lucene_dir>/<cache_name>/<index_id>/[lucene index files...]
}

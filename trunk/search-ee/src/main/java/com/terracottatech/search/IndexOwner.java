/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.LuceneIndexManager.AttributeProperties;

import java.util.List;
import java.util.Map;
import java.util.Timer;

interface IndexOwner {
  String getName();

  Timer getReaderRefreshTimer();

  void checkSchema(List<NVPair> attributes, boolean indexed) throws IndexException;

  Map<String, AttributeProperties> getSchema();
}

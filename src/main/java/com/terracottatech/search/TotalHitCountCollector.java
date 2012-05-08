/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public class TotalHitCountCollector extends Collector {

  private int hits;

  @Override
  public void setScorer(Scorer scorer) {
    //
  }

  @Override
  public void collect(int doc) {
    hits++;
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) {
    //
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  public int getTotalHits() {
    return hits;
  }

}

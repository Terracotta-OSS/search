/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.aggregator.Aggregator;

import java.util.Collections;
import java.util.List;

public class SearchResult<T extends IndexQueryResult> {

  public static final SearchResult NULL_RESULT = new SearchResult(0, Collections.EMPTY_LIST,
                                                                  Collections.EMPTY_LIST,
                                                                  false);
  /**
   * Total result count, pre-announce with response
   */
  private final long               totalResultCount;

  private final List<T>            queryResults;
  private final List<Aggregator>   aggregators;
  private final boolean            anyCriteriaMatch;

  public SearchResult(long resCt, List<T> queryResults, List<Aggregator> aggregators,
                      boolean anyCriteriaMatch) {
    this.totalResultCount = resCt;
    this.queryResults = queryResults;
    this.aggregators = aggregators;
    this.anyCriteriaMatch = anyCriteriaMatch;
  }

  public List<T> getQueryResults() {
    return queryResults;
  }

  public List<Aggregator> getAggregators() {
    return aggregators;
  }

  public boolean isAnyCriteriaMatch() {
    return anyCriteriaMatch;
  }

  public long getTotalResultCount() {
    return totalResultCount;
  }

}

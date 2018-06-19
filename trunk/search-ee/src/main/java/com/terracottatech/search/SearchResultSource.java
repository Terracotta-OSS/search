/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.aggregator.Aggregator;

import java.io.Closeable;
import java.util.List;

interface SearchResultSource extends Closeable {

  static SearchResultSource NULL_SOURCE = new SearchResultSource() {

                                          @Override
                                          public void acceptResult(DeferredQueryResult result) {
                                            throw new UnsupportedOperationException();
                                          }

                                          @Override
                                          public SearchResult<NonGroupedQueryResult> getResults(int start, int end) {
                                            throw new UnsupportedOperationException();
                                          }

                                          @Override
                                          public void close() {
                                            throw new UnsupportedOperationException();
                                          }

                                          @Override
                                          public void setAggregatorValues(List<Aggregator> aggregators) {
                                            throw new UnsupportedOperationException();
                                          }

                                        };

  void acceptResult(DeferredQueryResult result);

  <T extends IndexQueryResult> SearchResult<T> getResults(int start, int size) throws IndexException;

  void setAggregatorValues(List<Aggregator> aggregators);
}

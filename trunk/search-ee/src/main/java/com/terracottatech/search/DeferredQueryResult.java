/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.List;

class DeferredQueryResult implements NonGroupedQueryResult {
  private final NonGroupedQueryResult delegate;
  private final int              luceneDocId;

  DeferredQueryResult(NonGroupedQueryResult result, int docId) {
    delegate = result;
    luceneDocId = docId;
  }

  int getLuceneDocId() {
    return luceneDocId;
  }

  @Override
  public List<NVPair> getAttributes() {
    return delegate.getAttributes();
  }

  @Override
  public List<NVPair> getSortAttributes() {
    return delegate.getSortAttributes();
  }

  @Override
  public String getKey() {
    return delegate.getKey();
  }

  @Override
  public ValueID getValue() {
    return delegate.getValue();
  }

}

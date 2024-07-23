/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.util.List;
import java.util.Set;

import com.terracottatech.search.aggregator.Aggregator;

public interface GroupedQueryResult extends IndexQueryResult {

  public List<Aggregator> getAggregators();

  public Set<NVPair> getGroupedAttributes();
}

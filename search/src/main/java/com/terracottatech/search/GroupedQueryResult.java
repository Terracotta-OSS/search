/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.List;
import java.util.Set;

import com.terracottatech.search.aggregator.Aggregator;

public interface GroupedQueryResult extends IndexQueryResult {

  public List<Aggregator> getAggregators();

  public Set<NVPair> getGroupedAttributes();
}

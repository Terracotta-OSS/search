/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import com.terracottatech.search.aggregator.Aggregator;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class GroupedIndexQueryResultImpl extends IndexQueryResultImpl implements GroupedQueryResult {

  private final List<Aggregator> aggregators;
  private final Set<NVPair>      groupByAttributes;

  public GroupedIndexQueryResultImpl(List<NVPair> attributes, List<NVPair> sortAttributes,
                                     Set<NVPair> groupByAttributes, List<Aggregator> aggregatorResults) {
    super(attributes, sortAttributes);
    this.groupByAttributes = groupByAttributes;
    this.aggregators = aggregatorResults;
  }

  @Override
  public List<Aggregator> getAggregators() {
    return aggregators;
  }

  @Override
  public Set<NVPair> getGroupedAttributes() {
    return Collections.unmodifiableSet(groupByAttributes);
  }

}

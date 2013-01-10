/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SearchBuilder {

  private final List         queryStack     = new ArrayList();
  private final List<NVPair> aggregatorList = new ArrayList();
  private final Set<String>  attributes     = new HashSet<String>();
  private final Set<String>  groupByAttrs   = new HashSet<String>();
  private final List<NVPair> sortAttributes = new ArrayList();
  private boolean            includeKeys    = false;
  private boolean            includeValues  = false;
  private int                maxResults     = -1;

  public SearchBuilder() {
    //
  }

  public SearchBuilder includeKeys(boolean include) {
    this.includeKeys = include;
    return this;
  }

  public SearchBuilder includeValues(boolean include) {
    this.includeValues = include;
    return this;
  }

  public SearchBuilder attributeAscending(String attributeName) {
    this.sortAttributes.add(AbstractNVPair.createNVPair(attributeName, SortOperations.ASCENDING));
    return this;
  }

  public SearchBuilder attributeDescending(String attributeName) {
    this.sortAttributes.add(AbstractNVPair.createNVPair(attributeName, SortOperations.DESCENDING));
    return this;
  }

  public SearchBuilder groupBy(String attributeName) {
    this.groupByAttrs.add(attributeName);
    return this;
  }

  public SearchBuilder attribute(String attributeName) {
    this.attributes.add(attributeName);
    return this;
  }

  public SearchBuilder all() {
    add(StackOperations.ALL);
    return this;
  }

  public SearchBuilder and() {
    add(StackOperations.AND);
    return this;
  }

  public SearchBuilder or() {
    add(StackOperations.OR);
    return this;
  }

  public SearchBuilder beginGroup() {
    add(StackOperations.BEGIN_GROUP);
    return this;
  }

  public SearchBuilder endGroup() {
    add(StackOperations.END_GROUP);
    return this;
  }

  public SearchBuilder ilike(String name, Object value) {
    add(StackOperations.ILIKE);
    add(AbstractNVPair.createNVPair(name, value));
    return this;
  }

  public SearchBuilder notIlike(String name, String regex) {
    add(StackOperations.NOT_ILIKE);
    add(AbstractNVPair.createNVPair(name, regex));
    return this;
  }

  public SearchBuilder greaterThan(String name, Object value) {
    add(StackOperations.GREATER_THAN);
    add(AbstractNVPair.createNVPair(name, value));
    return this;
  }

  public SearchBuilder greaterThanEqual(String name, Object value) {
    add(StackOperations.GREATER_THAN_EQUAL);
    add(AbstractNVPair.createNVPair(name, value));
    return this;
  }

  public SearchBuilder lessThan(String name, Object value) {
    add(StackOperations.LESS_THAN);
    add(AbstractNVPair.createNVPair(name, value));
    return this;
  }

  public SearchBuilder lessThanEqual(String name, Object value) {
    add(StackOperations.LESS_THAN_EQUAL);
    add(AbstractNVPair.createNVPair(name, value));
    return this;
  }

  public SearchBuilder between(String name1, Object value1, String name2, Object value2, boolean minInclusive,
                               boolean maxInclusive) {
    add(StackOperations.BETWEEN);
    add(AbstractNVPair.createNVPair(name1, value1));
    add(AbstractNVPair.createNVPair(name2, value2));
    add(AbstractNVPair.createNVPair("MIN_INCLUSIVE", minInclusive));
    add(AbstractNVPair.createNVPair("MAX_INCLUSIVE", maxInclusive));
    return this;
  }

  public SearchBuilder term(String attributeName, Object value) {
    add(StackOperations.TERM);
    add(AbstractNVPair.createNVPair(attributeName, value));
    return this;
  }

  public SearchBuilder notEqualTerm(String attributeName, Object value) {
    add(StackOperations.NOT_TERM);
    add(AbstractNVPair.createNVPair(attributeName, value));
    return this;
  }

  public SearchBuilder count() {
    aggregatorList.add(AbstractNVPair.createNVPair("COUNT", AggregatorOperations.COUNT));
    return this;
  }

  public SearchBuilder sum(String attributeName) {
    aggregatorList.add(AbstractNVPair.createNVPair(attributeName, AggregatorOperations.SUM));
    return this;
  }

  public SearchBuilder average(String attributeName) {
    aggregatorList.add(AbstractNVPair.createNVPair(attributeName, AggregatorOperations.AVERAGE));
    return this;
  }

  public SearchBuilder max(String attributeName) {
    aggregatorList.add(AbstractNVPair.createNVPair(attributeName, AggregatorOperations.MAX));
    return this;
  }

  public SearchBuilder min(String attributeName) {
    aggregatorList.add(AbstractNVPair.createNVPair(attributeName, AggregatorOperations.MIN));
    return this;
  }

  public SearchBuilder maxResults(int max) {
    maxResults = max;
    return this;
  }

  // operations
  private void add(Object obj) {
    queryStack.add(obj);
  }

  public Search build() {
    return new Search(includeKeys, includeValues, queryStack, attributes, groupByAttrs, aggregatorList, sortAttributes,
                      maxResults);
  }

  public static class Search {

    private final boolean      includeKeys;
    private final boolean      includeValues;
    private final List         queryStack;
    private final Set<String>  attributes;
    private final Set<String>  groupByAttrs;
    private final List<NVPair> aggregatorList;
    private final List<NVPair> sortAttributes;
    private final int          maxResults;

    public Search(boolean includeKeys, boolean includeValues, List queryStack, Set<String> attributes,
                  Set<String> groupByAttrs, List<NVPair> aggregatorList, List<NVPair> sortAttributes, int maxResults) {
      this.includeKeys = includeKeys;
      this.includeValues = includeValues;
      this.queryStack = Collections.unmodifiableList(new ArrayList(queryStack));
      this.attributes = Collections.unmodifiableSet(new HashSet<String>(attributes));
      this.groupByAttrs = Collections.unmodifiableSet(new HashSet<String>(groupByAttrs));
      this.aggregatorList = Collections.unmodifiableList(new ArrayList<NVPair>(aggregatorList));
      this.sortAttributes = Collections.unmodifiableList(new ArrayList<NVPair>(sortAttributes));
      this.maxResults = maxResults;
    }

    public boolean isIncludeKeys() {
      return includeKeys;
    }

    public boolean isIncludeValues() {
      return includeValues;
    }

    public List getQueryStack() {
      return queryStack;
    }

    public Set<String> getAttributes() {
      return attributes;
    }

    public Set<String> getGroupByAttrs() {
      return groupByAttrs;
    }

    public List<NVPair> getAggregatorList() {
      return aggregatorList;
    }

    public List<NVPair> getSortAttributes() {
      return sortAttributes;
    }

    public int getMaxResults() {
      return maxResults;
    }
  }

}

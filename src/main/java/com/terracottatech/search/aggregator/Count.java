/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.AggregatorOperations;
import com.terracottatech.search.ValueType;

public class Count extends AbstractAggregator {

  private int count;

  public Count(String attributeName, ValueType type) {
    super(AggregatorOperations.COUNT, attributeName, type);
  }

  public void accept(Object input) {
    count++;
  }

  /**
   * Increment count by given number.
   * 
   * @param delta how much to increment by
   * @throws IllegalArgumentException if delta is negative
   */
  public void increment(int delta) throws IllegalArgumentException {
    if (delta < 0) throw new IllegalArgumentException("argument must not be negative");
    count += delta;
  }

  public void accept(Aggregator incoming) throws IllegalArgumentException {
    if (incoming instanceof Count) {
      count += ((Count) incoming).count;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public Integer getResult() {
    return Integer.valueOf(count);
  }

}

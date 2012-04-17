/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.AggregatorOperations;
import com.terracottatech.search.ValueType;

public class MinMax extends AbstractAggregator {

  private final boolean min;

  private Comparable    result;

  private MinMax(AggregatorOperations operation, String attributeName, boolean min, ValueType type) {
    super(operation, attributeName, type);
    this.min = min;
  }

  public static MinMax min(String attributeName, ValueType type) {
    return new MinMax(AggregatorOperations.MIN, attributeName, true, type);
  }

  public static MinMax max(String attributeName, ValueType type) {
    return new MinMax(AggregatorOperations.MAX, attributeName, false, type);
  }

  public Comparable getResult() {
    return result;
  }

  public void accept(Object input) throws IllegalArgumentException {
    if (input == null) { return; }

    Comparable next = getComparable(input);

    if (result == null) {
      result = next;
    } else {

      final int cmp;
      try {
        cmp = next.compareTo(result);
      } catch (ClassCastException cce) {
        throw new IllegalArgumentException(getAttributeName(), cce);
      }

      if (min) {
        if (cmp < 0) {
          result = next;
        }
      } else {
        if (cmp > 0) {
          result = next;
        }
      }
    }
  }

  public void accept(Aggregator incoming) throws IllegalArgumentException {
    if (incoming instanceof MinMax) {
      accept(((MinMax) incoming).result);
    } else {
      throw new IllegalArgumentException();
    }
  }

  private static Comparable getComparable(Object o) throws IllegalArgumentException {
    if (o instanceof Comparable) { return (Comparable) o; }

    throw new IllegalArgumentException("Value is not Comparable: " + o.getClass());
  }

}

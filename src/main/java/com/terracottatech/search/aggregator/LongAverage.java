/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.ValueType;

public class LongAverage extends Average {

  private long sum   = 0;
  private int  count = 0;

  public LongAverage(String attributeName, ValueType type) {
    super(attributeName, type);
  }

  public void accept(Object input) throws IllegalArgumentException {
    if (input == null) { return; }

    if (!(input instanceof Number)) { throw new IllegalArgumentException(input.getClass().getName()
                                                                         + " is not a number for attribute ["
                                                                         + getAttributeName() + "]"); }

    count++;
    sum += ((Number) input).longValue();
  }

  public void accept(Aggregator incoming) throws IllegalArgumentException {
    if (incoming instanceof LongAverage) {
      count += ((LongAverage) incoming).count;
      sum += ((LongAverage) incoming).sum;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public Double getResult() {
    if (count == 0) {
      return null;
    } else {
      return Double.valueOf(((double) sum) / count);
    }
  }

}

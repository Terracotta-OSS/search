/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.ValueType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleAverage extends Average {

  private double sum   = 0;
  private int    count = 0;

  public DoubleAverage(String attributeName, ValueType type) {
    super(attributeName, type);
  }

  public void accept(Object input) throws IllegalArgumentException {
    if (input == null) { return; }

    if (!(input instanceof Number)) { throw new IllegalArgumentException(input.getClass().getName()
                                                                         + " is not a number for attribute ["
                                                                         + getAttributeName() + "]"); }

    count++;
    sum += ((Number) input).doubleValue();
  }

  public void accept(Aggregator incoming) throws IllegalArgumentException {
    if (incoming instanceof DoubleAverage) {
      count += ((DoubleAverage) incoming).count;
      sum += ((DoubleAverage) incoming).sum;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public Double getResult() {
    if (count == 0) {
      return null;
    } else {
      return Double.valueOf(sum / count);
    }
  }

  @Override
  Aggregator deserializeData(DataInput input) throws IOException {
    sum = input.readDouble();
    count = input.readInt();
    return this;
  }

  @Override
  void serializeData(DataOutput output) throws IOException {
    output.writeDouble(sum);
    output.writeInt(count);
  }
}

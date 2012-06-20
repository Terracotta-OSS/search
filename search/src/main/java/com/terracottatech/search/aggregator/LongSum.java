/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.ValueType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongSum extends Sum {

  private boolean used = false;
  private long    sum  = 0;

  public LongSum(String attributeName, ValueType type) {
    super(attributeName, type);
  }

  public void accept(Object input) throws IllegalArgumentException {
    if (input == null) { return; }

    if (!(input instanceof Number)) { throw new IllegalArgumentException(input.getClass().getName()
                                                                         + " is not a number for attribute ["
                                                                         + getAttributeName() + "]"); }

    sum += ((Number) input).longValue();
    used = true;
  }

  public void accept(Aggregator incoming) throws IllegalArgumentException {
    if (incoming instanceof LongSum) {
      sum += ((LongSum) incoming).sum;
      used = true;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public Long getResult() {
    if (used) {
      return Long.valueOf(sum);
    } else {
      return null;
    }
  }

  @Override
  public Aggregator deserializeData(DataInput input) throws IOException {
    used = input.readBoolean();
    sum = input.readLong();
    return this;
  }

  @Override
  public void serializeData(DataOutput output) throws IOException {
    output.writeBoolean(used);
    output.writeLong(sum);
  }
}

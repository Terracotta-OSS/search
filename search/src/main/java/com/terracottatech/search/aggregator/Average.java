/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.AggregatorOperations;
import com.terracottatech.search.ValueType;

public abstract class Average extends AbstractAggregator {

  public Average(String attributeName, ValueType type) {
    super(AggregatorOperations.AVERAGE, attributeName, type);
  }

  public static Average average(String attributeName, ValueType type) throws IllegalArgumentException {
    if (type == null) {
      return new EmptyAverage(attributeName);
    } else {
      switch (type) {
        case BYTE:
        case CHAR:
        case SHORT:
        case INT:
          return new IntegerAverage(attributeName, type);
        case LONG:
          return new LongAverage(attributeName, type);
        case FLOAT:
          return new FloatAverage(attributeName, type);
        case DOUBLE:
          return new DoubleAverage(attributeName, type);
        default:
          throw new IllegalArgumentException("Attribute [" + attributeName + ":" + type + "] is not a numeric type");
      }
    }
  }
}

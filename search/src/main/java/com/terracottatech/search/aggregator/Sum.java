/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.AggregatorOperations;
import com.terracottatech.search.ValueType;

public abstract class Sum extends AbstractAggregator {

  public Sum(String attributeName, ValueType type) {
    super(AggregatorOperations.SUM, attributeName, type);
  }

  public static Sum sum(String attributeName, ValueType type) throws IllegalArgumentException {
    if (type == null) {
      return new EmptySum(attributeName);
    } else {
      switch (type) {
        case BYTE:
        case CHAR:
        case SHORT:
        case INT:
        case LONG:
          return new LongSum(attributeName, type);
        case FLOAT:
          return new FloatSum(attributeName, type);
        case DOUBLE:
          return new DoubleSum(attributeName, type);
        default:
          throw new IllegalArgumentException("Attribute [" + attributeName + ":" + type + "] is not a numeric type");
      }
    }
  }
}

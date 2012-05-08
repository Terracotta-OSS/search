/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.AggregatorOperations;
import com.terracottatech.search.ValueType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class AbstractAggregator implements Aggregator {

  private static final ValueType[]   ALL_VALUE_TYPES = ValueType.values();

  private final String               attributeName;
  private final ValueType            type;
  private final AggregatorOperations operation;

  AbstractAggregator(AggregatorOperations operation, String attributeName, ValueType type) {
    this.attributeName = attributeName;
    this.type = type;
    this.operation = operation;
  }

  public final String getAttributeName() {
    return attributeName;
  }

  public final ValueType getType() {
    return type;
  }

  public final AggregatorOperations getOperation() {
    return operation;
  }

  public final void serializeTo(DataOutput output) throws IOException {
    output.writeUTF(getAttributeName());
    if (type == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(type.ordinal());
    }
    output.writeInt(operation.ordinal());
    serializeData(output);
  }

  public final Object deserializeFrom(DataInput input) throws IOException {
    return deserializeInstance(input);
  }

  abstract Aggregator deserializeData(DataInput input) throws IOException;

  abstract void serializeData(DataOutput output) throws IOException;

  public static Aggregator deserializeInstance(DataInput input) throws IOException {
    String attributeName = input.readUTF();
    ValueType type;
    int typeIndex = input.readInt();
    if (typeIndex < 0) {
      type = null;
    } else {
      type = ALL_VALUE_TYPES[typeIndex];
    }
    AggregatorOperations operation = AggregatorOperations.values()[input.readInt()];

    return aggregator(operation, attributeName, type).deserializeData(input);
  }

  public static AbstractAggregator aggregator(AggregatorOperations operation, String attributeName, ValueType type) {
    switch (operation) {
      case AVERAGE:
        return Average.average(attributeName, type);
      case COUNT:
        return new Count(attributeName, type);
      case MAX:
        return MinMax.max(attributeName, type);
      case MIN:
        return MinMax.min(attributeName, type);
      case SUM:
        return Sum.sum(attributeName, type);
    }
    throw new IllegalArgumentException();
  }
}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.AbstractNVPair;
import com.terracottatech.search.AggregatorOperations;
import com.terracottatech.search.ValueType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
    if (getType() == ValueType.ENUM) {
      // this needs to happen here to happen in the correct classloader context (not in deserialize)
      return (Comparable) AbstractNVPair.createNVPair(getAttributeName(), result, getType()).getObjectValue();
    }
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

  @Override
  Aggregator deserializeData(DataInput input) throws IOException {
    boolean isNull = input.readBoolean();
    if (isNull) {
      result = null;
      return this;
    }

    switch (getType()) {
      case BOOLEAN:
        result = input.readBoolean();
        return this;
      case BYTE:
        result = input.readByte();
        return this;
      case BYTE_ARRAY:
        throw new AssertionError();
      case CHAR:
        result = input.readChar();
        return this;
      case DATE:
        result = new java.util.Date(input.readLong());
        return this;
      case DOUBLE:
        result = input.readDouble();
        return this;
      case ENUM:
        result = input.readUTF();
        return this;
      case FLOAT:
        result = input.readFloat();
        return this;
      case INT:
        result = input.readInt();
        return this;
      case LONG:
        result = input.readLong();
        return this;
      case NULL:
        throw new AssertionError();
      case SHORT:
        result = input.readShort();
        return this;
      case SQL_DATE:
        result = new java.sql.Date(input.readLong());
        return this;
      case STRING:
        result = input.readUTF();
        return this;
      case VALUE_ID:
        throw new AssertionError();
    }

    throw new AssertionError(getType());
  }

  @Override
  void serializeData(DataOutput output) throws IOException {
    if (result == null) {
      output.writeBoolean(true);
      return;
    }

    output.writeBoolean(false);

    switch (getType()) {
      case BOOLEAN:
        output.writeBoolean((Boolean) result);
        return;
      case BYTE:
        output.writeByte((Byte) result);
        return;
      case BYTE_ARRAY:
        throw new AssertionError();
      case CHAR:
        output.writeChar((Character) result);
        return;
      case DATE:
        output.writeLong(((java.util.Date) result).getTime());
        return;
      case DOUBLE:
        output.writeDouble((Double) result);
        return;
      case ENUM:
        if (result instanceof String) {
          output.writeUTF((String) result);
        } else if (result instanceof Enum) {
          output.writeUTF(AbstractNVPair.enumStorageString((Enum) result));
        } else {
          throw new AssertionError("Unexpected type: " + result.getClass());
        }
        return;
      case FLOAT:
        output.writeFloat((Float) result);
        return;
      case INT:
        output.writeInt((Integer) result);
        return;
      case LONG:
        output.writeLong((Long) result);
        return;
      case NULL:
        throw new AssertionError();
      case SHORT:
        output.writeShort((Short) result);
        return;
      case SQL_DATE:
        output.writeLong(((java.sql.Date) result).getTime());
        return;
      case STRING:
        output.writeUTF((String) result);
        return;
      case VALUE_ID:
        throw new AssertionError();
    }

    throw new AssertionError(getType());
  }
}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;

import com.terracottatech.search.ValueType;

import java.io.DataOutput;
import java.io.IOException;

public interface Aggregator {

  void accept(Object input) throws IllegalArgumentException;

  void accept(Aggregator incoming) throws IllegalArgumentException;

  String getAttributeName();

  Object getResult();

  ValueType getType();

  void serializeTo(DataOutput out) throws IOException;
}

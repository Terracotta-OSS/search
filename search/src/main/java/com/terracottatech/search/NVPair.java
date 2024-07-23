/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.io.Serializable;

public interface NVPair extends Serializable, Comparable<NVPair> {

  String getName();

  ValueType getType();

  Object getObjectValue();

  NVPair cloneWithNewName(String newName);

  NVPair cloneWithNewValue(Object newValue);

  // XXX: remove this from the interface?
  String valueAsString();

}

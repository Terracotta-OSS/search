/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.io.Serializable;

public class ValueID implements Serializable {

  public static final ValueID NULL_ID = new ValueID(-1);

  private final long          value;

  public ValueID(long value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + value + ")";
  }

  public boolean isNull() {
    return this == NULL_ID;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (value ^ (value >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ValueID other = (ValueID) obj;
    if (value != other.value) return false;
    return true;
  }

  public long toLong() {
    return value;
  }

}

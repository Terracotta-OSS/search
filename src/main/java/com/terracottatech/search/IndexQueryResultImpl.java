/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.Collections;
import java.util.List;

public class IndexQueryResultImpl implements IndexQueryResult, Comparable {

  private final String       key;
  private final List<NVPair> attributes;
  private final List<NVPair> sortAttributes;
  private final ValueID      valueID;

  public IndexQueryResultImpl(String key, ValueID valueID, List<NVPair> attributes, List<NVPair> sortAttributes) {
    this.key = key;
    this.valueID = valueID;
    this.attributes = attributes;
    this.sortAttributes = sortAttributes;
  }

  /**
   * {@inheritDoc}
   */
  public String getKey() {
    return this.key;
  }

  public ValueID getValue() {
    return valueID;
  }

  /**
   * {@inheritDoc}
   */
  public List<NVPair> getAttributes() {
    return Collections.unmodifiableList(this.attributes);
  }

  /**
   * {@inheritDoc}
   */
  public List<NVPair> getSortAttributes() {
    return Collections.unmodifiableList(this.sortAttributes);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    IndexQueryResultImpl other = (IndexQueryResultImpl) obj;
    if (key == null) {
      if (other.key != null) return false;
    } else if (!key.equals(other.key)) return false;
    return true;
  }

  public int compareTo(Object o) {
    if (this == o) return 0;
    if (o == null) return -1;
    if (getClass() != o.getClass()) return -1;
    IndexQueryResultImpl other = (IndexQueryResultImpl) o;
    if (key == null) {
      if (other.key != null) return -1;
    }
    return other.key.compareTo(key);
  }

  @Override
  public String toString() {
    return new StringBuilder(256).append("<").append(getClass().getSimpleName()).append(": key=").append(key)
        .append(" value=").append(valueID).append(" attributes=").append(attributes).append(" sortAttributes=")
        .append(sortAttributes).append(">").toString();
  }

}

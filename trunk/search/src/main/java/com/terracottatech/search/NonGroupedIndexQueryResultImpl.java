/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.List;

public class NonGroupedIndexQueryResultImpl extends IndexQueryResultImpl implements NonGroupedQueryResult {

  private final String  key;
  private final ValueID valueID;

  public NonGroupedIndexQueryResultImpl(String key, ValueID valueOID, List<NVPair> attributes,
                                        List<NVPair> sortAttributes) {
    super(attributes, sortAttributes);
    this.key = key;
    this.valueID = valueOID;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getKey() {
    return this.key;
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public ValueID getValue() {
    return valueID;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    NonGroupedIndexQueryResultImpl other = (NonGroupedIndexQueryResultImpl) obj;
    if (key == null) {
      if (other.key != null) return false;
    } else if (!key.equals(other.key)) return false;
    return true;
  }

  @Override
  public String toString() {
    return new StringBuilder(256).append("<").append(getClass().getSimpleName()).append(": key=").append(key)
        .append(" value=").append(valueID).append(" attributes=").append(getAttributes()).append(" sortAttributes=")
        .append(getSortAttributes()).append(">").toString();
  }

}

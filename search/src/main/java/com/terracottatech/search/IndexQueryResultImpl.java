/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.util.Collections;
import java.util.List;

public abstract class IndexQueryResultImpl implements IndexQueryResult {

  private final List<NVPair> attributes;
  private final List<NVPair> sortAttributes;

  protected IndexQueryResultImpl(List<NVPair> attributes, List<NVPair> sortAttributes) {
    this.attributes = attributes;
    this.sortAttributes = sortAttributes;
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

}

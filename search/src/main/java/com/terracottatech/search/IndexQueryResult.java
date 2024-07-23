/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.util.List;

/**
 * Index search result.
 * 
 */
public interface IndexQueryResult extends SortFieldProvider {

  /**
   * Entry attributes.
   * 
   * @return attributes
   */
  List<NVPair> getAttributes();

}

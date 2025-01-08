/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

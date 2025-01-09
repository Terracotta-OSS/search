/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search;

public interface NonGroupedQueryResult extends IndexQueryResult {
  /**
   * Entry key.
   * 
   * @return key
   */
  String getKey();

  /**
   * Entry value.
   * 
   * @return value
   */
  ValueID getValue();

}

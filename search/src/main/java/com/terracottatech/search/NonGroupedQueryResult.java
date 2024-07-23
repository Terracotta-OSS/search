/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

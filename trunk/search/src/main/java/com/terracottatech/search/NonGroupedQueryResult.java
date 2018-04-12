/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
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

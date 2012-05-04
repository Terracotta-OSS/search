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
  public String getKey();

  /**
   * Entry value.
   * 
   * @return value
   */
  public ValueID getValue();

}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.List;

/**
 * Index search result.
 * 
 * @author Nabib El-Rahman
 */
public interface IndexQueryResult {

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

  /**
   * Entry attributes.
   * 
   * @return attributes
   */
  public List<NVPair> getAttributes();

  /**
   * Entry sortAttributes.
   * 
   * @return attributes
   */
  public List<NVPair> getSortAttributes();
}

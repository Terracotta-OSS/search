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

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
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

/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.util.List;

interface SortFieldProvider {
  /**
   * Entry sortAttributes.
   * 
   * @return attributes
   */
  List<NVPair> getSortAttributes();

}

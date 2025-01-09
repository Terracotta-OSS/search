/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

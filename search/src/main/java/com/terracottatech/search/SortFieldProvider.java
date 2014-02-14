/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
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

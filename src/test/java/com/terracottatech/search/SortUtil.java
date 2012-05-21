/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.terracottatech.search.AbstractNVPair.StringNVPair;

import java.util.List;

class SortUtil {
  static void checkStringSortOrder(SearchResult res, String attrName, boolean desc) {
    String val = null;
    List<IndexQueryResult> results = res.getQueryResults();
    for (IndexQueryResult result : results) {
      val = verifyOrder(val, result, attrName, desc);
    }

  }

  static void checkStringSortOrder(SearchResult res, String keyPrefix, String attrName, boolean desc) {

    String val = null;
    List<NonGroupedQueryResult> results = res.getQueryResults();
    for (NonGroupedQueryResult result : results) {
      boolean hasKey = false;
      if (result.getKey().startsWith(keyPrefix)) {
        hasKey = true;
      }
      val = verifyOrder(val, result, attrName, desc);
      assertTrue(hasKey);
    }

  }

  private static String verifyOrder(String prev, IndexQueryResult res, String attrName, boolean desc) {
    for (NVPair pair : res.getAttributes()) {
      if (attrName.equals(pair.getName())) {
        Object value = pair.getObjectValue();
        if (value == null) {
          assertNull(prev);
        } else {
          StringNVPair strPair = (StringNVPair) pair;
          String attr = strPair.getValue();
          System.out.println(attr);
          if (prev != null) assertTrue(attr, attr.compareTo(prev) * (desc ? -1 : 1) >= 0);
          prev = attr;
        }
      }
    }
    return prev;

  }

}

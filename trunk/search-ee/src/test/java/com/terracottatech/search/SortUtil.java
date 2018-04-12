/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import static org.junit.Assert.assertTrue;

import com.terracottatech.search.AbstractNVPair.NullNVPair;

import java.util.List;

class SortUtil {
  static void checkStringSortOrder(List<? extends IndexQueryResult> results, String attrName, boolean desc) {
    NVPair val = null;
    for (IndexQueryResult result : results) {
      val = verifyOrder(val, result, attrName, desc);
    }

  }

  static void checkStringSortOrder(List<NonGroupedQueryResult> results, String keyPrefix, String attrName, boolean desc) {

    NVPair val = null;
    for (NonGroupedQueryResult result : results) {
      boolean hasKey = false;
      if (result.getKey().startsWith(keyPrefix)) {
        hasKey = true;
      }
      val = verifyOrder(val, result, attrName, desc);
      assertTrue(hasKey);
    }

  }

  private static NVPair verifyOrder(NVPair prev, IndexQueryResult res, String attrName, boolean desc) {
    for (NVPair pair : res.getAttributes()) {
      if (attrName.equals(pair.getName())) {
        Object value = pair.getObjectValue();
        if (value == null && !desc) {
          assertTrue(String.valueOf(prev), prev == null || prev instanceof NullNVPair);
        } else {
          if (prev != null) assertTrue(pair.toString(), pair.compareTo(prev) * (desc ? -1 : 1) >= 0);
          prev = pair;
        }
      }
    }
    return prev;

  }

}

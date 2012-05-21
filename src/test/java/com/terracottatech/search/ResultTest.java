/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class ResultTest {

  private final static int           stripeCt      = 2;

  Collection<List<IndexQueryResult>> stripeResults = new ArrayList<List<IndexQueryResult>>(stripeCt);
  String[]                           sortValues;

  @Before
  public void init() {
    stripeResults.clear();
    for (int i = 0; i < stripeCt; i++) {
      stripeResults.add(new LinkedList<IndexQueryResult>());
    }
    sortValues = new String[] { "ron", "jon", "don", "tom", "juan" };
  }

  @Test
  public void testDescMergeSort() {
    Arrays.sort(sortValues);
    List<String> descOrder = Arrays.asList(sortValues);
    Collections.reverse(descOrder);
    Random r = new Random();
    for (List<IndexQueryResult> stripeRes : stripeResults) {
      int len = r.nextInt(sortValues.length) + 1;
      int offset = r.nextInt(sortValues.length - len + 1);
      for (int n = 0; n < len; n++) {
        stripeRes.add(new NonGroupedIndexQueryResultImpl(null, ValueID.NULL_ID, Collections.EMPTY_LIST, Collections
            .singletonList(AbstractNVPair.createNVPair("name", descOrder.get(offset + n)))));
      }
    }
    NVPair sortBy = new AbstractNVPair.EnumNVPair("name", SortOperations.DESCENDING);
    List<IndexQueryResult> sorted = ResultTools.mergeSort(stripeResults, Collections.singletonList(sortBy));
    SortUtil.checkStringSortOrder(new SearchResult<IndexQueryResult>(sorted, null, true), "name", true);
  }

  @Test
  public void testAscMergeSort() {
    Arrays.sort(sortValues);
    List<String> descOrder = Arrays.asList(sortValues);
    Random r = new Random();
    for (List<IndexQueryResult> stripeRes : stripeResults) {
      int len = r.nextInt(sortValues.length) + 1;
      int offset = r.nextInt(sortValues.length - len + 1);
      for (int n = 0; n < len; n++) {
        stripeRes.add(new NonGroupedIndexQueryResultImpl(null, ValueID.NULL_ID, Collections.EMPTY_LIST, Collections
            .singletonList(AbstractNVPair.createNVPair("name", descOrder.get(offset + n)))));
      }
    }
    NVPair sortBy = new AbstractNVPair.EnumNVPair("name", SortOperations.ASCENDING);
    List<IndexQueryResult> sorted = ResultTools.mergeSort(stripeResults, Collections.singletonList(sortBy));
    SortUtil.checkStringSortOrder(new SearchResult<IndexQueryResult>(sorted, null, true), "name", false);
  }

}

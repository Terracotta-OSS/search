/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.junit.Test;

import com.terracottatech.search.SearchBuilder.Search;
import com.terracottatech.search.aggregator.Aggregator;
import com.terracottatech.search.aggregator.MinMax;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

public class AggregatorTest extends SearchTestBase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    String name = getName();
    {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(AbstractNVPair.createNVPair("units", TimeUnit.DAYS));
      attributes.add(AbstractNVPair.createNVPair("gender", Gender.MALE));

      idxManager.insert(name, "key-1", new ValueID(1L), attributes, EMPTY, 10, new NullProcessingContext());
    }

    {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(AbstractNVPair.createNVPair("units", TimeUnit.HOURS));
      attributes.add(AbstractNVPair.createNVPair("gender", Gender.FEMALE));

      idxManager.insert(name, "key-2", new ValueID(2L), attributes, EMPTY, 7, new NullProcessingContext());
    }

  }

  @Test
  public void testNoLimit() throws IndexException {
    Search s = new SearchBuilder().min("units").max("gender").build();
    SearchResult res = idxManager.searchIndex(getName(), s);
    Assert.assertEquals(0, res.getTotalResultCount());
    verifyAggs(res.getAggregators());
  }

  @Test
  public void testLimit() throws IndexException {
    Search s = new SearchBuilder().min("units").max("gender").maxResults(2).build();
    SearchResult res = idxManager.searchIndex(getName(), s);

    List<IndexQueryResult> results = res.getQueryResults();
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(2, results.get(0).getAttributes().size());

    verifyAggs(res.getAggregators());
  }

  private void verifyAggs(List<Aggregator> aggs) {
    Assert.assertEquals(2, aggs.size());
    MinMax min = (MinMax) aggs.get(0);
    Assert.assertTrue(min.getResult() instanceof TimeUnit);
    Assert.assertTrue(TimeUnit.class.equals(((Enum) min.getResult()).getDeclaringClass()));

    MinMax max = (MinMax) aggs.get(1);
    Assert.assertTrue(max.getResult() instanceof Gender);
    Assert.assertTrue(Gender.class.equals(((Enum) max.getResult()).getDeclaringClass()));
  }

  private static enum Gender {
    MALE, FEMALE
  }
}

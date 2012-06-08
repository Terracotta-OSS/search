/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.store.FSDirectory;

import com.terracottatech.search.AbstractNVPair.ByteArrayNVPair;
import com.terracottatech.search.AbstractNVPair.IntNVPair;
import com.terracottatech.search.AbstractNVPair.StringNVPair;
import com.terracottatech.search.aggregator.Aggregator;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;
import junit.framework.TestCase;

public class LuceneIndexManagerTest extends TestCase {

  private static final List<NVPair> EMPTY         = Collections.emptyList();
  private static final int          idxCt         = 4;

  private LuceneIndexManager        idxManager;

  private final Configuration       cfg           = new Configuration(idxCt, 10, false, false, true, 0, 0, 0);
  private final LoggerFactory       loggerFactory = new SysOutLoggerFactory();

  @Override
  public void tearDown() throws Exception {
    if (idxManager != null) idxManager.shutdown();
    // Util.deleteDirectory(getLuceneDir());
  }

  @Override
  public void setUp() throws Exception {
    idxManager = newIndexManager(getLuceneDir(true));
  }

  private LuceneIndexManager newIndexManager(File dir) {
    return new LuceneIndexManager(dir, false, loggerFactory, cfg);
  }

  public void testOptimize() throws Exception {
    assertEquals(0, idxManager.getSearchIndexNames().length);

    idxManager.insert("foo", "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());

    assertEquals(1, idxManager.getSearchIndexNames().length);
    assertEquals("foo", idxManager.getSearchIndexNames()[0]);

    idxManager.optimizeSearchIndex("foo");

    idxManager.optimizeSearchIndex("foo (does not exist!)");
  }

  public void testIncompleteIndex() throws Exception {
    File luceneDir = getLuceneDir(false);
    File cacheDir = new File(luceneDir, "foo");
    File indexDir = new File(cacheDir, "1");
    assertFalse(cacheDir.exists());

    final IOException toThrow = new IOException();
    try {
      new LuceneIndex(FSDirectory.open(indexDir), "foo", indexDir, false, null, cfg, loggerFactory) {
        @Override
        protected void createInitFile() throws IOException {
          throw toThrow;
        }
      };
      fail();
    } catch (IndexException ie) {
      assertEquals(toThrow, ie.getCause());
    }

    assertTrue(indexDir.isDirectory());
    FileOutputStream out = new FileOutputStream(new File(cacheDir, LuceneIndexManager.TERRACOTTA_CACHE_NAME_FILE));
    out.write(cacheDir.getName().getBytes("UTF-16"));
    out.flush();
    out.close();

    File schemaFile = new File(cacheDir, LuceneIndexManager.TERRACOTTA_SCHEMA_FILE);
    assertTrue(schemaFile.createNewFile());
    out = new FileOutputStream(schemaFile);
    Properties props = new Properties();
    props.store(out, null);
    out.close();

    idxManager.init();
    assertEquals(0, idxManager.getSearchIndexNames().length);
    assertFalse(indexDir.exists());
  }

  public void testSplitIndex() throws Exception {
    File luceneDir = getLuceneDir(false);

    assertEquals(0, idxManager.getSearchIndexNames().length);

    int docCt = idxCt * 2;
    for (int i = 0; i < docCt; i++) {
      idxManager.insert("foo", "key", new ValueID(12), EMPTY, EMPTY, i, new NullProcessingContext());
    }

    assertEquals(1, idxManager.getSearchIndexNames().length);
    assertEquals("foo", idxManager.getSearchIndexNames()[0]);

    File cacheDir = new File(luceneDir, "foo");
    assertTrue(cacheDir.isDirectory());
    assertEquals(idxCt, cacheDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File path) {
        return path.isDirectory();
      }
    }).length);

    for (File idx : cacheDir.listFiles()) {
      if (idx.isDirectory()) assertTrue(idx.list().length > 0);
    }

  }

  public void testSimpleSearch() throws IndexException {
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    List<NVPair> storeOnlyattributes = new ArrayList<NVPair>();
    storeOnlyattributes.add(new AbstractNVPair.ByteArrayNVPair("attr2", new byte[] { 6, 6, 6 }));

    ValueID valueOid = new ValueID(1);

    int docCt = idxCt * 2;
    for (int i = 0; i < docCt; i++) {
      idxManager.insert("foo", "key-" + i, valueOid, attributes, storeOnlyattributes, i, new NullProcessingContext());
    }

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");
    attributeSet.add("attr2");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex("foo", queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  EMPTY, EMPTY, -1);
    boolean hasKey = false;
    List<NonGroupedQueryResult> res = context.getQueryResults();
    assertEquals(docCt, res.size());
    for (NonGroupedQueryResult result : res) {
      if (result.getKey().startsWith("key-")) {
        hasKey = true;
      }
      assertEquals(valueOid, result.getValue());
      for (NVPair pair : result.getAttributes()) {
        if (pair.getName().equals("attr1")) {
          StringNVPair stringNVPair = (StringNVPair) pair;
          assertEquals("foo", stringNVPair.getValue());
        } else if (pair.getName().equals("attr2")) {
          ByteArrayNVPair byteArrayNVPair = (ByteArrayNVPair) pair;
          assertTrue(Arrays.equals(new byte[] { 6, 6, 6 }, byteArrayNVPair.getValue()));
        } else {
          throw new AssertionError(pair);
        }
      }
    }
    assertTrue(hasKey);
  }

  public void testClear() throws Exception {

    List<NVPair> attributes = Collections.EMPTY_LIST;
    String name = getName();

    idxManager.insert(name, "key1", new ValueID(100), attributes, EMPTY, 1, new NullProcessingContext());
    idxManager.insert(name, "key2", new ValueID(200), attributes, EMPTY, 2, new NullProcessingContext());

    List queryStack = new LinkedList();
    queryStack.add(StackOperations.ALL);
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET,
                                                  Collections.EMPTY_SET, Collections.EMPTY_LIST,
                                                  Collections.EMPTY_LIST, -1);
    assertEquals(2, context.getQueryResults().size());

    // clear an segment that does not exist (should not change anything)
    idxManager.clear(name, 666, new NullProcessingContext());
    context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_SET,
                                     Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    assertEquals(2, context.getQueryResults().size());

    // clear segment 1
    idxManager.clear(name, 1, new NullProcessingContext());
    context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_SET,
                                     Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    assertEquals(1, context.getQueryResults().size());

    // clear segment 2
    idxManager.clear(name, 2, new NullProcessingContext());
    context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_SET,
                                     Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    assertEquals(0, context.getQueryResults().size());

  }

  public void testEvictorRemove() throws IndexException {
    String name = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    idxManager.insert(name, "key", valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());
    Map remove = new HashMap();

    remove.put("key", valueOid);
    idxManager.removeIfValueEqual(name, remove, 1985L /* wrong segment */, new NullProcessingContext());

    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    // Removal is no-op
    assertEquals(1, context.getQueryResults().size());

    remove.put("key", new ValueID(666));
    idxManager.removeIfValueEqual(name, remove, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                     Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    assertEquals(1, context.getQueryResults().size());

    remove.put("key", valueOid);
    idxManager.removeIfValueEqual(name, remove, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET, EMPTY, EMPTY,
                                     -1);
    assertEquals(0, context.getQueryResults().size());

  }

  public void testRemove() throws Exception {
    String name = getName();
    String key = "id";

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    idxManager.insert(name, key, valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());
    idxManager.remove(name, key, 1985L /* wrong segment */, new NullProcessingContext());

    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    // Removal is no-op
    assertEquals(1, context.getQueryResults().size());

    idxManager.remove(name, "boo", segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                     Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    assertEquals(1, context.getQueryResults().size());

    idxManager.remove(name, key, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                     new ArrayList<NVPair>(), new ArrayList<NVPair>(), -1);
    assertEquals(0, context.getQueryResults().size());

  }

  public void testStringSort() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    String[] names = { "larry", "gary", "mary", "barry" };

    // Same segment
    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", names[0]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[1]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 43));
    idxManager.insert(idx, "p1", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    // Missing value for sort field
    attributes.add(new AbstractNVPair.IntNVPair("age", 19));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[3]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 27));
    idxManager.insert(idx, "p3", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    // Missing value for sort field
    attributes.add(new AbstractNVPair.IntNVPair("age", 62));
    idxManager.insert(idx, "p4", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("name");
    attributeSet.add("age");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> sortAttributes = new ArrayList<NVPair>();
    sortAttributes.add(AbstractNVPair.createNVPair("name", SortOperations.DESCENDING));

    SearchResult context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  sortAttributes, Collections.EMPTY_LIST, -1);
    SortUtil.checkStringSortOrder(context, "p", "name", true);

    sortAttributes.clear();
    sortAttributes.add(AbstractNVPair.createNVPair("name", SortOperations.ASCENDING));
    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     Collections.EMPTY_LIST, -1);
    SortUtil.checkStringSortOrder(context, "p", "name", false);

    // Across multiple segments/indexes
    attributes.add(new AbstractNVPair.StringNVPair("name", "jerry"));
    attributes.add(new AbstractNVPair.IntNVPair("age", 36));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, EMPTY, 1985L, new NullProcessingContext());

    // Missing value for sort field
    attributes.clear();
    attributes.add(new AbstractNVPair.IntNVPair("age", 32));
    idxManager.insert(idx, "p1", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     Collections.EMPTY_LIST, -1);
    SortUtil.checkStringSortOrder(context, "p", "name", false);

    sortAttributes.clear();
    sortAttributes.add(AbstractNVPair.createNVPair("name", SortOperations.DESCENDING));
    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     Collections.EMPTY_LIST, -1);
    SortUtil.checkStringSortOrder(context, "p", "name", true);

  }

  public void testNumericSort() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    String[] names = { "larry", "gary", "mary", "jerry" };

    // Same segment
    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", names[0]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[1]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 43));
    idxManager.insert(idx, "p1", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[3]));
    // Missing value to sort by
    idxManager.insert(idx, "p3", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[2]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 27));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[0]));
    // Missing value to sort by
    idxManager.insert(idx, "p4", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("name");
    attributeSet.add("age");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> sortAttributes = new ArrayList<NVPair>();
    sortAttributes.add(AbstractNVPair.createNVPair("age", SortOperations.ASCENDING));

    SearchResult context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  sortAttributes, Collections.EMPTY_LIST, -1);

    checkNumSortOrder(context, "p", "age");

    // Across multiple segments/indexes
    attributes.add(new AbstractNVPair.StringNVPair("name", names[2]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 36));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, EMPTY, 1985L, new NullProcessingContext());

    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[1]));
    // Missing value to sort by
    idxManager.insert(idx, "p3", new ValueID(20), attributes, EMPTY, 1985, new NullProcessingContext());

    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     Collections.EMPTY_LIST, -1);
    checkNumSortOrder(context, "p", "age");

    // Add missing field to sort order
    sortAttributes.add(AbstractNVPair.createNVPair("address", SortOperations.ASCENDING));
    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     Collections.EMPTY_LIST, -1);
    // This should just work the same as if the extra sort field was never specified
    checkNumSortOrder(context, "p", "age");
  }

  public void testUnknownFieldSort() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();

    // Same segment
    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", "foo"));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, EMPTY, segmentId, new NullProcessingContext());

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> sortAttributes = new ArrayList<NVPair>();

    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("name");
    attributeSet.add("age");
    sortAttributes.add(AbstractNVPair.createNVPair("address", SortOperations.ASCENDING));
    SearchResult<IndexQueryResult> context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet,
                                                                    Collections.EMPTY_SET, sortAttributes,
                                                                    Collections.EMPTY_LIST, -1);

    IndexQueryResult res = context.getQueryResults().get(0);
    assertEquals(1, res.getSortAttributes().size());
    assertNull(res.getSortAttributes().get(0).getObjectValue());
  }

  public void testBuiltInFuctionBasic() throws IndexException {
    String name = getName();
    int minVal = 100;
    int count = idxCt * 3;
    int maxVal = minVal + count - 1;

    for (int i = 0; i < count; i++) {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));
      if (i % 2 == 0) attributes.add(AbstractNVPair.createNVPair("name", "Sir" + Character.valueOf((char) ('A' + i))));
      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, EMPTY, i, new NullProcessingContext());
    }

    // test functions
    Set<String> attributeSet = Collections.EMPTY_SET;
    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);
    List<NVPair> sortAttributes = Collections.EMPTY_LIST;

    List<NVPair> aggregatorList = new ArrayList<NVPair>();
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MAX));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MIN));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.COUNT));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.SUM));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.AVERAGE));
    // Create a second count aggregator, for a good measure
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.COUNT));

    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  sortAttributes, aggregatorList, -1);

    List<Aggregator> results = context.getAggregators();
    System.out.println("Max : " + results.get(0).getResult());
    assertEquals(maxVal, results.get(0).getResult());
    System.out.println("Min : " + results.get(1).getResult());
    assertEquals(minVal, results.get(1).getResult());
    System.out.println("Count : " + results.get(2).getResult());
    assertEquals(count, results.get(2).getResult());
    System.out.println("Sum : " + results.get(3).getResult());
    assertEquals(Long.valueOf(count * (minVal + maxVal) / 2), results.get(3).getResult());
    System.out.println("Average : " + results.get(4).getResult());
    assertEquals(minVal + (maxVal - minVal) / 2f, results.get(4).getResult());
    System.out.println("Count : " + results.get(5).getResult());
    assertEquals(count, results.get(5).getResult());

    // Expect to get something back in an aggregator anyway, even when aggregating on non-existent field
    aggregatorList.add(AbstractNVPair.createNVPair("name", AggregatorOperations.MAX));
    queryStack.clear();
    queryStack.add(StackOperations.BEGIN_GROUP);
    queryStack.add(StackOperations.OR);
    for (int i = 1; i < count; i += 2) {
      queryStack.add(StackOperations.TERM);
      queryStack.add(new AbstractNVPair.IntNVPair("age", minVal + i));
    }
    queryStack.add(StackOperations.END_GROUP);

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     aggregatorList, -1);
    results = context.getAggregators();
    assertEquals(7, results.size());
    assertNull(results.get(6).getResult());
  }

  public void testAggregatorsLimitResSet() throws IndexException {
    String name = getName();
    int minVal = 100;
    int count = idxCt * 3;
    int maxVal = minVal + count - 1;
    for (int i = 0; i < count; i++) {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));
      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, EMPTY, i, new NullProcessingContext());
    }

    // test functions
    Set<String> attributeSet = Collections.EMPTY_SET;
    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);
    List<NVPair> sortAttributes = new LinkedList<NVPair>();
    sortAttributes.add(AbstractNVPair.createNVPair("age", SortOperations.DESCENDING));

    List<NVPair> aggregatorList = new ArrayList<NVPair>();
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.COUNT));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MAX));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MIN));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.SUM));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.AVERAGE));

    // Run size-limited search, with given sort order
    int limit = 4;
    int min = maxVal - limit + 1;

    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  sortAttributes, aggregatorList, limit);

    List<Aggregator> results = context.getAggregators();
    System.out.println("Count : " + results.get(0).getResult());
    assertEquals(limit, results.get(0).getResult());
    System.out.println("Max : " + results.get(1).getResult());
    assertEquals(maxVal, results.get(1).getResult());
    System.out.println("Min : " + results.get(2).getResult());
    assertEquals(min, results.get(2).getResult());
    System.out.println("Sum : " + results.get(3).getResult());
    assertEquals(Long.valueOf(limit * (min + maxVal) / 2), results.get(3).getResult());
    System.out.println("Average : " + results.get(4).getResult());
    assertEquals(min + (maxVal - min) / 2f, results.get(4).getResult());

  }

  public void testMaxResults() throws IndexException {
    String name = getName();
    int minVal = 100;
    int count = idxCt * 3;

    List<NVPair> sortAttributes = new ArrayList<NVPair>();
    sortAttributes.add(new AbstractNVPair.EnumNVPair("age", SortOperations.DESCENDING));

    for (int i = 0; i < count; i++) {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));

      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, EMPTY, i, new NullProcessingContext());
    }

    Set<String> attributeSet = Collections.EMPTY_SET;
    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> aggregatorList = Collections.EMPTY_LIST;

    // top 2 results, from sorted list
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  sortAttributes, aggregatorList, 2);

    List<NonGroupedQueryResult> res = context.getQueryResults();
    System.out.println(res);
    assertEquals("key" + (count - 1), res.get(0).getKey());
    assertEquals("key" + (count - 2), res.get(1).getKey());
    assertEquals(2, res.size());

    // any 2 results - unsorted list
    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                     Collections.EMPTY_LIST, aggregatorList, 2);

    assertEquals(2, context.getQueryResults().size());

    // All results; max > result set
    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET, sortAttributes,
                                     aggregatorList, count + 1);

    assertEquals(count, context.getQueryResults().size());

  }

  public void testReplace() throws IndexException {
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    idxManager.replace("foo", "replace", new ValueID(2), new ValueID(1), attributes, EMPTY, 0,
                       new NullProcessingContext());

    List<IndexQueryResult> res = searchResults();
    assertEquals(0, res.size());
    idxManager.insert("foo", "replace", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());
    res = searchResults();
    assertEquals(1, res.size());

    for (int i = 0; i < 10; i++) {
      idxManager.replace("foo", "replace", new ValueID(i + 2), new ValueID(i + 1), attributes, EMPTY, 0,
                         new NullProcessingContext());
    }
    assertEquals(1, res.size());
  }

  public void testUpdateKey() throws IndexException {
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    attributes.add(new AbstractNVPair.IntNVPair("numeric", 1));

    idxManager.insert("foo", "key", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("numeric", 1));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult searchResult = idxManager.searchIndex("foo", queryStack, true, true, Collections.EMPTY_SET,
                                                       Collections.EMPTY_SET, EMPTY, EMPTY, -1);

    List<NonGroupedQueryResult> res = searchResult.getQueryResults();
    assertEquals(1, res.size());
    assertEquals("key", res.get(0).getKey());

    idxManager.updateKey("foo", "key", "newKey", 0, new NullProcessingContext());
    res = idxManager.searchIndex("foo", queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_SET, EMPTY,
                                 EMPTY, -1).getQueryResults();
    assertEquals(1, res.size());
    assertEquals("newKey", res.get(0).getKey());
  }

  public void testIncompleteIndexGroup() throws IndexException, IOException {
    FileFilter dirsOnly = new FileFilter() {
      @Override
      public boolean accept(File path) {
        return path.isDirectory();
      }
    };

    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    idxManager.insert("foo", "replace", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());
    Assert.assertEquals(idxCt, new File(getLuceneDir(), "foo").listFiles(dirsOnly).length);

    verifyClean(dirsOnly);

    idxManager.init();

    verifyClean(dirsOnly);

    idxManager.shutdown();

    Random r = new Random();
    int removeIndex = r.nextInt(idxCt);

    Util.deleteDirectory(new File(getLuceneDir().getAbsolutePath() + File.separator + "foo", Integer
        .toString(removeIndex)));

    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();
    Assert.assertEquals(0, getLuceneDir().listFiles(dirsOnly).length);

    idxManager.insert("xyz", "new value", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());

    verifyClean(dirsOnly);

    idxManager.shutdown();
    File init = new File(getLuceneDir().getAbsolutePath() + File.separator + "xyz" + File.separator
                         + Integer.toString(removeIndex), LuceneIndex.TERRACOTTA_INIT_FILE);
    boolean deleted = init.delete();
    assertTrue(init.getAbsolutePath(), deleted);

    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();

    Assert.assertEquals(0, getLuceneDir().listFiles(dirsOnly).length);
  }

  private void verifyClean(FileFilter dirsOnly) throws IOException {

    Assert.assertEquals(1, getLuceneDir().listFiles(dirsOnly).length);

    for (File dir : getLuceneDir().listFiles(dirsOnly)) {
      Assert.assertEquals(idxCt, dir.listFiles(dirsOnly).length);
      for (File subDir : dir.listFiles(dirsOnly)) {
        Assert.assertTrue(LuceneIndex.hasInitFile(subDir));
      }
    }
  }

  private List<IndexQueryResult> searchResults() throws IndexException {
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex("foo", queryStack, true, true, attributeSet, Collections.EMPTY_SET,
                                                  EMPTY, EMPTY, -1);
    return context.getQueryResults();
  }

  private File getLuceneDir(boolean clean) throws IOException {
    File dir = new File(getTempDirectory(), getName());
    if (clean) {
      Util.cleanDirectory(dir);
    }
    return dir;
  }

  public void testGroupBySearch() throws IndexException {
    String name = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    String[] names = { "larry", "gary", "mary", "barry", "terry" };
    String[] states = { "CA", "ID", "MD" };
    String[] cities = { "Campbell", "Springfield" };

    for (int i = 0; i < names.length * 3; i++) {
      attributes.add(new AbstractNVPair.StringNVPair("name", names[i % names.length]));
      attributes.add(new AbstractNVPair.IntNVPair("age", 40 + 3 * i * (i % 2 > 0 ? -1 : 1)));
      attributes.add(new AbstractNVPair.StringNVPair("state", states[i % states.length]));
      attributes.add(new AbstractNVPair.StringNVPair("city", cities[i % cities.length]));
      idxManager
          .insert(name, "p" + i, new ValueID(i), attributes, EMPTY, i % names.length, new NullProcessingContext());
      attributes.clear();
    }

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("state");

    Set<String> groupByAttrs = new HashSet<String>();
    groupByAttrs.add("state");
    groupByAttrs.add("city");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("age", 3));
    queryStack.addFirst(StackOperations.GREATER_THAN);

    List<NVPair> aggregatorList = new ArrayList<NVPair>();
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MIN));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.COUNT));

    List<NVPair> sortAttributes = new ArrayList<NVPair>();
    sortAttributes.add(AbstractNVPair.createNVPair("state", SortOperations.DESCENDING));

    SearchResult<GroupedQueryResult> context = idxManager.searchIndex(name, queryStack, false, false, attributeSet,
                                                                      groupByAttrs, sortAttributes, aggregatorList, -1);
    SortUtil.checkStringSortOrder(context, "state", true);
    for (GroupedQueryResult res : context.getQueryResults()) {
      Assert.assertEquals(1, res.getAttributes().size());
      Assert.assertEquals("state", res.getAttributes().get(0).getName());
      for (NVPair attr : res.getGroupedAttributes()) {
        System.out.print(attr.getName() + ": " + attr.getObjectValue() + "  ");
      }

      for (Aggregator agg : res.getAggregators()) {
        System.out.print(agg.getResult() + "  ");
      }
      System.out.println();
    }
  }

  private File getLuceneDir() throws IOException {
    return getLuceneDir(false);
  }

  private String getTempDirectory() {
    File testTempDir = new File("target/temp", getClass().getSimpleName());
    testTempDir.mkdirs();
    return testTempDir.getAbsolutePath();
  }

  /**
   * Verify ascending numeric sort order
   */
  private void checkNumSortOrder(SearchResult res, String keyPrefix, String attrName) {
    Integer n = Integer.MIN_VALUE;
    List<NonGroupedQueryResult> results = res.getQueryResults();
    for (NonGroupedQueryResult result : results) {
      boolean hasKey = false;
      if (result.getKey().startsWith(keyPrefix)) {
        hasKey = true;
      }
      for (NVPair pair : result.getSortAttributes()) {
        if (attrName.equals(pair.getName())) {
          Object value = pair.getObjectValue();
          if (value == null) {
            assertEquals(Integer.MIN_VALUE, (int) n);
            System.out.println(n);
          } else {
            IntNVPair intPair = (IntNVPair) pair;
            int num = intPair.getValue();
            System.out.println(num);
            assertTrue(String.valueOf(num), num >= n);
            n = num;
          }
        }
      }
      assertTrue(hasKey);
    }

  }

}

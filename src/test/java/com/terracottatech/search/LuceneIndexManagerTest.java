/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.store.FSDirectory;

import com.terracottatech.search.AbstractNVPair.IntNVPair;
import com.terracottatech.search.AbstractNVPair.StringNVPair;
import com.terracottatech.search.aggregator.Aggregator;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

public class LuceneIndexManagerTest extends TestCase {

  private static final int   idxCt = 4;

  private LuceneIndexManager idxManager;
  private Configuration      cfg;
  private LoggerFactory      loggerFactory;

  @Override
  public void tearDown() throws Exception {
    if (idxManager != null) idxManager.shutdown();
    // Util.deleteDirectory(getLuceneDir());
  }

  @Override
  public void setUp() throws Exception {
    loggerFactory = new SysOutLoggerFactory();

    // getProperties().setProperty(TCPropertiesConsts.SEARCH_LUCENE_INDEXES_PER_CACHE, String.valueOf(idxCt));
    cfg = new Configuration(idxCt, 10, false, false, true);

    idxManager = new LuceneIndexManager(getLuceneDir(), false, loggerFactory, cfg);
  }

  public void testOptimize() throws Exception {

    assertEquals(0, idxManager.getSearchIndexNames().length);

    idxManager.insert("foo", "key", new ValueID(12), Collections.EMPTY_LIST, 34, new NullProcessingContext());

    assertEquals(1, idxManager.getSearchIndexNames().length);
    assertEquals("foo", idxManager.getSearchIndexNames()[0]);

    idxManager.optimizeSearchIndex("foo");

    idxManager.optimizeSearchIndex("foo (does not exist!)");
  }

  public void testIncompleteIndex() throws Exception {
    File luceneDir = getLuceneDir();
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
    File luceneDir = getLuceneDir();

    assertEquals(0, idxManager.getSearchIndexNames().length);

    int docCt = idxCt * 2;
    for (int i = 0; i < docCt; i++) {
      idxManager.insert("foo", "key", new ValueID(12), Collections.EMPTY_LIST, i, new NullProcessingContext());
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

  public void testSimpleSearch() throws IOException, IndexException {
    File luceneDir = getLuceneDir();
    Util.deleteDirectory(luceneDir);

    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);

    int docCt = idxCt * 2;
    for (int i = 0; i < docCt; i++) {
      idxManager.insert("foo", "key-" + i, valueOid, attributes, i, new NullProcessingContext());
    }

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex("foo", queryStack, true, true, attributeSet, new ArrayList<NVPair>(),
                                                  new ArrayList<NVPair>(), -1);
    boolean hasKey = false;
    List<IndexQueryResult> res = context.getQueryResults();
    assertEquals(docCt, res.size());
    for (IndexQueryResult result : res) {
      if (result.getKey().startsWith("key-")) {
        hasKey = true;
      }
      assertEquals(valueOid, result.getValue());
      for (NVPair pair : result.getAttributes()) {
        StringNVPair stringNVPair = (StringNVPair) pair;
        assertEquals("attr1", stringNVPair.getName());
        assertEquals("foo", stringNVPair.getValue());
      }
    }
    assertTrue(hasKey);
  }

  public void testClear() throws Exception {

    List<NVPair> attributes = Collections.EMPTY_LIST;
    String name = getName();

    idxManager.insert(name, "key1", new ValueID(100), attributes, 1, new NullProcessingContext());
    idxManager.insert(name, "key2", new ValueID(200), attributes, 2, new NullProcessingContext());

    List queryStack = new LinkedList();
    queryStack.add(StackOperations.ALL);
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET,
                                                  Collections.EMPTY_LIST, Collections.EMPTY_LIST, -1);
    assertEquals(2, context.getQueryResults().size());

    // clear an segment that does not exist (should not change anything)
    idxManager.clear(name, 666, new NullProcessingContext());
    context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_LIST,
                                     Collections.EMPTY_LIST, -1);
    assertEquals(2, context.getQueryResults().size());

    // clear segment 1
    idxManager.clear(name, 1, new NullProcessingContext());
    context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_LIST,
                                     Collections.EMPTY_LIST, -1);
    assertEquals(1, context.getQueryResults().size());

    // clear segment 2
    idxManager.clear(name, 2, new NullProcessingContext());
    context = idxManager.searchIndex(name, queryStack, true, true, Collections.EMPTY_SET, Collections.EMPTY_LIST,
                                     Collections.EMPTY_LIST, -1);
    assertEquals(0, context.getQueryResults().size());

  }

  public void testEvictorRemove() throws IndexException {
    String name = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    idxManager.insert(name, "key", valueOid, attributes, segmentId, new NullProcessingContext());
    Map remove = new HashMap();

    remove.put("key", valueOid);
    idxManager.removeIfValueEqual(name, remove, 1985L /* wrong segment */, new NullProcessingContext());

    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_LIST,
                                                  Collections.EMPTY_LIST, -1);
    // Removal is no-op
    assertEquals(1, context.getQueryResults().size());

    remove.put("key", new ValueID(666));
    idxManager.removeIfValueEqual(name, remove, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_LIST,
                                     Collections.EMPTY_LIST, -1);
    assertEquals(1, context.getQueryResults().size());

    remove.put("key", valueOid);
    idxManager.removeIfValueEqual(name, remove, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, new ArrayList<NVPair>(),
                                     new ArrayList<NVPair>(), -1);
    assertEquals(0, context.getQueryResults().size());

  }

  public void testRemove() throws Exception {
    String name = getName();
    String key = "id";

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    idxManager.insert(name, key, valueOid, attributes, segmentId, new NullProcessingContext());
    idxManager.remove(name, key, 1985L /* wrong segment */, new NullProcessingContext());

    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_LIST,
                                                  Collections.EMPTY_LIST, -1);
    // Removal is no-op
    assertEquals(1, context.getQueryResults().size());

    idxManager.remove(name, "boo", segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_LIST,
                                     Collections.EMPTY_LIST, -1);
    assertEquals(1, context.getQueryResults().size());

    idxManager.remove(name, key, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, new ArrayList<NVPair>(),
                                     new ArrayList<NVPair>(), -1);
    assertEquals(0, context.getQueryResults().size());

  }

  public void testStringSort() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    String[] names = { "larry", "gary", "mary" };

    // Same segment
    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", names[0]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[1]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 43));
    idxManager.insert(idx, "p1", new ValueID(20), attributes, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[2]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 27));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, segmentId, new NullProcessingContext());
    attributes.clear();

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("name");
    attributeSet.add("age");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> sortAttributes = new ArrayList<NVPair>();
    sortAttributes.add(AbstractNVPair.createNVPair("name", SortOperations.DESCENDING));

    SearchResult context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, sortAttributes,
                                                  Collections.EMPTY_LIST, -1);
    checkStringSortOrder(context, "p", "name", true);

    sortAttributes.clear();
    sortAttributes.add(AbstractNVPair.createNVPair("name", SortOperations.ASCENDING));
    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, sortAttributes, Collections.EMPTY_LIST,
                                     -1);
    checkStringSortOrder(context, "p", "name", false);

    // Across multiple segments/indexes
    attributes.add(new AbstractNVPair.StringNVPair("name", "jerry"));
    attributes.add(new AbstractNVPair.IntNVPair("age", 36));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, 1985L, new NullProcessingContext());

    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, sortAttributes, Collections.EMPTY_LIST,
                                     -1);
    checkStringSortOrder(context, "p", "name", false);

    sortAttributes.clear();
    sortAttributes.add(AbstractNVPair.createNVPair("name", SortOperations.DESCENDING));
    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, sortAttributes, Collections.EMPTY_LIST,
                                     -1);
    checkStringSortOrder(context, "p", "name", true);

  }

  public void testNumericSort() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    String[] names = { "larry", "gary", "mary" };

    // Same segment
    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", names[0]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[1]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 43));
    idxManager.insert(idx, "p1", new ValueID(20), attributes, segmentId, new NullProcessingContext());
    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[2]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 27));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, segmentId, new NullProcessingContext());
    attributes.clear();

    // test search
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("name");
    attributeSet.add("age");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> sortAttributes = new ArrayList<NVPair>();
    sortAttributes.add(AbstractNVPair.createNVPair("age", SortOperations.ASCENDING));

    SearchResult context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, sortAttributes,
                                                  Collections.EMPTY_LIST, -1);

    checkNumSortOrder(context, "p", "age");

    // Across multiple segments/indexes
    attributes.add(new AbstractNVPair.StringNVPair("name", names[2]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 36));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, 1985L, new NullProcessingContext());

    context = idxManager.searchIndex(idx, queryStack, true, true, attributeSet, sortAttributes, Collections.EMPTY_LIST,
                                     -1);
    checkNumSortOrder(context, "p", "age");
  }

  public void testBuiltInFuctionBasic() throws IndexException {
    String name = getName();
    int minVal = 100;
    int count = idxCt * 3;
    int maxVal = minVal + count - 1;
    for (int i = 0; i < count; i++) {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));
      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, i, new NullProcessingContext());
    }

    // test functions
    Set<String> attributeSet = Collections.EMPTY_SET;
    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);
    List<NVPair> sortAttributes = Collections.EMPTY_LIST;

    List<NVPair> aggregatorList = new ArrayList<NVPair>();
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.COUNT));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MAX));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MIN));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.SUM));
    aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.AVERAGE));

    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, sortAttributes,
                                                  aggregatorList, -1);

    List<Aggregator> results = context.getAggregators();
    System.out.println("Count : " + results.get(0).getResult());
    assertEquals(count, results.get(0).getResult());
    System.out.println("Max : " + results.get(1).getResult());
    assertEquals(maxVal, results.get(1).getResult());
    System.out.println("Min : " + results.get(2).getResult());
    assertEquals(minVal, results.get(2).getResult());
    System.out.println("Sum : " + results.get(3).getResult());
    assertEquals(Long.valueOf(count * (minVal + maxVal) / 2), results.get(3).getResult());
    System.out.println("Average : " + results.get(4).getResult());
    assertEquals(minVal + (maxVal - minVal) / 2f, results.get(4).getResult());
  }

  public void testAggregatorsLimitResSet() throws IndexException {
    String name = getName();
    int minVal = 100;
    int count = idxCt * 3;
    int maxVal = minVal + count - 1;
    for (int i = 0; i < count; i++) {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));
      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, i, new NullProcessingContext());
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

    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, sortAttributes,
                                                  aggregatorList, limit);

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

      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, i, new NullProcessingContext());
    }

    Set<String> attributeSet = Collections.EMPTY_SET;
    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(StackOperations.ALL);

    List<NVPair> aggregatorList = Collections.EMPTY_LIST;

    // top 2 results, from sorted list
    SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, sortAttributes,
                                                  aggregatorList, 2);

    List<IndexQueryResult> res = context.getQueryResults();
    System.out.println(res);
    assertEquals("key" + (count - 1), res.get(0).getKey());
    assertEquals("key" + (count - 2), res.get(1).getKey());
    assertEquals(2, res.size());

    // any 2 results - unsorted list
    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_LIST,
                                     aggregatorList, 2);

    assertEquals(2, context.getQueryResults().size());

    // All results; max > result set
    context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, sortAttributes, aggregatorList,
                                     count + 1);

    assertEquals(count, context.getQueryResults().size());

  }

  public void testReplace() throws IndexException {
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    idxManager.replace("foo", "replace", new ValueID(2), new ValueID(1), attributes, 0, new NullProcessingContext());

    List<IndexQueryResult> res = searchResults();
    assertEquals(0, res.size());
    idxManager.insert("foo", "replace", new ValueID(1), attributes, 0, new NullProcessingContext());
    res = searchResults();
    assertEquals(1, res.size());

    for (int i = 0; i < 10; i++) {
      idxManager.replace("foo", "replace", new ValueID(i + 2), new ValueID(i + 1), attributes, 0,
                         new NullProcessingContext());
    }
    assertEquals(1, res.size());
  }

  private List<IndexQueryResult> searchResults() throws IndexException {
    Set<String> attributeSet = new HashSet<String>();
    attributeSet.add("attr1");

    LinkedList queryStack = new LinkedList();
    queryStack.addFirst(AbstractNVPair.createNVPair("attr1", "foo"));
    queryStack.addFirst(StackOperations.TERM);
    SearchResult context = idxManager.searchIndex("foo", queryStack, true, true, attributeSet, new ArrayList<NVPair>(),
                                                  new ArrayList<NVPair>(), -1);
    return context.getQueryResults();
  }

  private File getLuceneDir() throws IOException {
    File dir = new File(getTempDirectory(), getName());
    Util.cleanDirectory(dir);
    return dir;
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
    int n = 0;
    for (IndexQueryResult result : res.getQueryResults()) {
      boolean hasKey = false;
      if (result.getKey().startsWith(keyPrefix)) {
        hasKey = true;
      }
      for (NVPair pair : result.getAttributes()) {
        if (attrName.equals(pair.getName())) {
          IntNVPair intPair = (IntNVPair) pair;
          int num = intPair.getValue();
          System.out.println(num);
          assertTrue(String.valueOf(num), num >= n);
          n = num;
        }
      }
      assertTrue(hasKey);
    }

  }

  private void checkStringSortOrder(SearchResult res, String keyPrefix, String attrName, boolean desc) {

    String val = null;
    for (IndexQueryResult result : res.getQueryResults()) {
      boolean hasKey = false;
      if (result.getKey().startsWith(keyPrefix)) {
        hasKey = true;
      }
      for (NVPair pair : result.getAttributes()) {
        if (attrName.equals(pair.getName())) {
          StringNVPair strPair = (StringNVPair) pair;
          String attr = strPair.getValue();
          System.out.println(attr);
          if (val != null) assertTrue(attr, attr.compareTo(val) * (desc ? -1 : 1) >= 0);
          val = attr;
        }
      }
      assertTrue(hasKey);
    }

  }

}

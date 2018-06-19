/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.AbstractNVPair.ByteArrayNVPair;
import com.terracottatech.search.AbstractNVPair.IntNVPair;
import com.terracottatech.search.AbstractNVPair.StringNVPair;
import com.terracottatech.search.LuceneIndexManager.AttributeProperties;
import com.terracottatech.search.SearchBuilder.Search;
import com.terracottatech.search.aggregator.Aggregator;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.hamcrest.core.Is.is;

public class LuceneIndexManagerTest {

  @Rule
  public TestName                   testName      = new TestName();

  private static final List<NVPair> EMPTY         = Collections.emptyList();
  private static final int          idxCt         = 4;

  private LuceneIndexManager        idxManager;

  private final Configuration       cfg           = new Configuration(idxCt, 10, false, false, true, 0, 0, 0);
  private final LoggerFactory       loggerFactory = new SysOutLoggerFactory();

  @After
  public void tearDown() throws Exception {
    if (idxManager != null) idxManager.shutdown();
    Util.deleteDirectory(getLuceneDir());
  }

  @Before
  public void setUp() throws Exception {
    idxManager = newIndexManager(getLuceneDir(true));
  }

  private LuceneIndexManager newIndexManager(File dir) {
    cfg.setDoAccessChecks(false);
    return new LuceneIndexManager(dir, true, loggerFactory, cfg);
  }

  @Test
  public void testOptimize() throws Exception {
    assertEquals(0, idxManager.getSearchIndexNames().length);

    idxManager.insert("foo", "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());

    assertEquals(1, idxManager.getSearchIndexNames().length);
    assertEquals("foo", idxManager.getSearchIndexNames()[0]);

    idxManager.optimizeSearchIndex("foo");

    idxManager.optimizeSearchIndex("foo (does not exist!)");
  }

  @Test
  public void testIncompleteIndex() throws Exception {
    File luceneDir = getLuceneDir(false);
    File cacheDir = new File(luceneDir, "foo");
    File indexDir = new File(cacheDir, "1");
    assertFalse(cacheDir.exists());

    final IOException toThrow = new IOException();
    try {
      new LuceneIndex(FSDirectory.open(indexDir), "foo", indexDir, new IndexOwner() {

        @Override
        public String getName() {
          return "unknown";
        }

        @Override
        public Timer getReaderRefreshTimer() {
          return new Timer(true);
        }

        @Override
        public Map<String, AttributeProperties> getSchema() {
          return null;
        }

        @Override
        public void checkSchema(List<NVPair> attributes, boolean indexed) {
          // do nothing
        }
      }, cfg, loggerFactory) {
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
    FileOutputStream out = new FileOutputStream(new File(cacheDir, SearchConsts.TERRACOTTA_CACHE_NAME_FILE));
    out.write(cacheDir.getName().getBytes("UTF-16"));
    out.flush();
    out.close();

    File schemaFile = new File(cacheDir, SearchConsts.TERRACOTTA_SCHEMA_FILE);
    assertTrue(schemaFile.createNewFile());
    out = new FileOutputStream(schemaFile);
    Properties props = new Properties();
    props.store(out, null);
    out.close();

    idxManager.init();
    assertEquals(0, idxManager.getSearchIndexNames().length);
    assertFalse(indexDir.exists());
  }

  @Test
  public void testSplitIndex() throws Exception {
    File luceneDir = getLuceneDir(false);

    assertEquals(0, idxManager.getSearchIndexNames().length);

    int docCt = idxCt * 2;
    for (int i = 0; i < docCt; i++) {
      idxManager.insert("foo", "key", new ValueID(12), EMPTY, EMPTY, i, new NullProcessingContext());
    }

    assertEquals(1, idxManager.getSearchIndexNames().length);
    assertEquals("foo", idxManager.getSearchIndexNames()[0]);
    //Use Sanitized names instead of raw names
    File cacheDir = new File(luceneDir, Util.sanitizeCacheName("foo"));
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

  @Test
  public void testDestroy() throws IndexException {
    String name = "foo";
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);

    for (int i = 0; i < 10; i++) {
      idxManager
          .insert(name, "key-" + i, valueOid, attributes, Collections.EMPTY_LIST, i,
                        new NullProcessingContext());
    }

    List<IndexQueryResult> result = searchResults(name);
    assertEquals(10, result.size());

    idxManager.destroy(name, new NullProcessingContext());

    result = searchResults(name);
    assertEquals(0, result.size());

    for (int i = 0; i < 10; i++) {
      idxManager
          .insert(name, "key-" + i, valueOid, attributes, Collections.EMPTY_LIST, i,
                        new NullProcessingContext());
    }

    result = searchResults(name);
    assertEquals(10, result.size());
  }

  @Test
  public void testSimpleSearch() throws IndexException {
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "FooBar"));

    List<NVPair> storeOnlyattributes = new ArrayList<NVPair>();
    storeOnlyattributes.add(new AbstractNVPair.ByteArrayNVPair("attr2", new byte[] { 6, 6, 6 }));

    ValueID valueOid = new ValueID(1);

    int docCt = idxCt * 2;
    for (int i = 0; i < docCt; i++) {
      idxManager.insert("foo", "key-" + i, valueOid, attributes, storeOnlyattributes, i,
                        new NullProcessingContext());
    }

    // Verify case insensitive string matching
    Search s = new SearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").attribute("attr2")
        .term("attr1", "fOObar").build();
    // test search
    SearchResult context = idxManager.searchIndex("foo", s);
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
          assertEquals("FooBar", stringNVPair.getValue());
        } else if (pair.getName().equals("attr2")) {
          ByteArrayNVPair byteArrayNVPair = (ByteArrayNVPair) pair;
          assertTrue(Arrays.equals(new byte[] { 6, 6, 6 }, byteArrayNVPair.getValue()));
        } else {
          throw new AssertionError(pair);
        }
      }
    }
    assertTrue(hasKey);
    Assert.assertThat(idxManager.searchMonitor.getLiveQuerySnapshot().size(), is(0));
  }

  @Test
  public void testClear() throws Exception {

    List<NVPair> attributes = Collections.EMPTY_LIST;
    String name = getName();

    idxManager.insert(name, "key1", new ValueID(100), attributes, EMPTY, 1, new NullProcessingContext());
    idxManager.insert(name, "key2", new ValueID(200), attributes, EMPTY, 2, new NullProcessingContext());

    List queryStack = new LinkedList();
    queryStack.add(StackOperations.ALL);

    Search s = new SearchBuilder().all().includeKeys(true).build();
    SearchResult context = idxManager.searchIndex(name, s);
    assertEquals(2, context.getQueryResults().size());

    // clear an segment that does not exist (should not change anything)
    idxManager.clear(name, 666, new NullProcessingContext());
    context = idxManager.searchIndex(name, s);
    assertEquals(2, context.getQueryResults().size());

    // clear segment 1
    idxManager.clear(name, 1, new NullProcessingContext());
    context = idxManager.searchIndex(name, s);
    assertEquals(1, context.getQueryResults().size());

    // clear segment 2
    idxManager.clear(name, 2, new NullProcessingContext());
    context = idxManager.searchIndex(name, s);
    assertEquals(0, context.getQueryResults().size());

  }

  private String getName() {
    return testName.getMethodName();
  }

  @Test
  public void testEvictorRemove() throws IndexException {
    String name = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    idxManager.insert(name, "key", valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());
    Map<String, ValueID> remove = new HashMap<String, ValueID>();
    String key = "key";

    remove.put(key, valueOid);
    idxManager.removeIfValueEqual(name, remove, 1985L /* wrong segment */, new NullProcessingContext(), false);

    List<IndexQueryResult> context = searchResults(name);
    // Removal is no-op
    assertEquals(1, context.size());

    remove.put(key, new ValueID(666));
    idxManager.removeIfValueEqual(name, remove, segmentId, new NullProcessingContext(), false);

    context = searchResults(name);
    assertEquals(1, context.size());

    remove.put(key, valueOid);
    idxManager.removeIfValueEqual(name, remove, segmentId, new NullProcessingContext(), false);

    context = searchResults(name);
    assertEquals(0, context.size());

  }

  @Test
  public void testPutIfAbsent() throws IndexException {
    String name = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    String key = "key";
    idxManager.insert(name, key, valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());

    Search s = new SearchBuilder().attribute("attr1").includeKeys(true).term("attr1", "foo").build();
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(name, s);
    assertEquals(1, context.getQueryResults().size());

    attributes.add(new AbstractNVPair.StringNVPair("attr2", "bar"));
    idxManager.putIfAbsent(name, key, valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());

    s = new SearchBuilder().attribute("attr1").includeKeys(true).term("attr2", "bar").build();
    context = idxManager.searchIndex(name, s);
    assertEquals(0, context.getQueryResults().size());

    idxManager.remove(name, key, segmentId, new NullProcessingContext());
    idxManager.putIfAbsent(name, key, valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());

    context = idxManager.searchIndex(name, s);
    assertEquals(1, context.getQueryResults().size());
    assertEquals(key, context.getQueryResults().get(0).getKey());
    assertEquals("foo", context.getQueryResults().get(0).getAttributes().get(0).valueAsString());
  }

  @Test
  public void testRemove() throws Exception {
    String name = getName();
    String key = "id";

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    ValueID valueOid = new ValueID(1);
    long segmentId = 1984L;

    idxManager.insert(name, key, valueOid, attributes, EMPTY, segmentId, new NullProcessingContext());
    idxManager.remove(name, key, 1985L /* wrong segment */, new NullProcessingContext());

    // Removal is no-op
    assertEquals(1, searchResults(name).size());

    idxManager.remove(name, "boo", segmentId, new NullProcessingContext());

    assertEquals(1, searchResults(name).size());

    idxManager.remove(name, key, segmentId, new NullProcessingContext());

    assertEquals(0, searchResults(name).size());

  }

  @Test
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

    // Empty string for sort field - same behavior as missing/null
    attributes.add(new AbstractNVPair.IntNVPair("age", 62));
    attributes.add(new AbstractNVPair.StringNVPair("name", ""));
    idxManager.insert(idx, "p4", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    // test search

    Search s = new SearchBuilder().all().includeKeys(true).includeValues(true).attribute("name").attribute("age")
        .attributeDescending("name").build();

    SearchResult context = idxManager.searchIndex(idx, s);
    SortUtil.checkStringSortOrder(context.getQueryResults(), "p", "name", true);

    s = new SearchBuilder().all().includeKeys(true).includeValues(true).attribute("name").attribute("age")
        .attributeAscending("name").build();

    context = idxManager.searchIndex(idx, s);
    SortUtil.checkStringSortOrder(context.getQueryResults(), "p", "name", false);

    // Across multiple segments/indexes
    attributes.add(new AbstractNVPair.StringNVPair("name", "jerry"));
    attributes.add(new AbstractNVPair.IntNVPair("age", 36));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, EMPTY, 1985L, new NullProcessingContext());

    // Missing value for sort field
    attributes.clear();
    attributes.add(new AbstractNVPair.IntNVPair("age", 32));
    idxManager.insert(idx, "p1", new ValueID(20), attributes, EMPTY, segmentId, new NullProcessingContext());
    attributes.clear();

    context = idxManager.searchIndex(idx, s);
    SortUtil.checkStringSortOrder(context.getQueryResults(), "p", "name", false);

    s = new SearchBuilder().all().includeKeys(true).includeValues(true).attribute("name").attribute("age")
        .attributeDescending("name").build();
    context = idxManager.searchIndex(idx, s);
    SortUtil.checkStringSortOrder(context.getQueryResults(), "p", "name", true);

  }

  @Test
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
    Search s = new SearchBuilder().all().includeKeys(true).includeValues(true).attribute("name").attribute("age")
        .attributeAscending("age").build();

    SearchResult context = idxManager.searchIndex(idx, s);

    checkNumSortOrder(context, "p", "age");

    // Across multiple segments/indexes
    attributes.add(new AbstractNVPair.StringNVPair("name", names[2]));
    attributes.add(new AbstractNVPair.IntNVPair("age", 36));
    idxManager.insert(idx, "p2", new ValueID(20), attributes, EMPTY, 1985L, new NullProcessingContext());

    attributes.clear();

    attributes.add(new AbstractNVPair.StringNVPair("name", names[1]));
    // Missing value to sort by
    idxManager.insert(idx, "p3", new ValueID(20), attributes, EMPTY, 1985, new NullProcessingContext());

    context = idxManager.searchIndex(idx, s);
    checkNumSortOrder(context, "p", "age");

    // Add missing field to sort order
    s = new SearchBuilder().all().includeKeys(true).includeValues(true).attribute("name").attribute("age")
        .attributeDescending("name").attributeAscending("address").build();

    context = idxManager.searchIndex(idx, s);
    // This should just work the same as if the extra sort field was never specified
    checkNumSortOrder(context, "p", "age");
  }

  @Test
  public void testUnknownFieldSort() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();

    // Same segment
    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", "foo"));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, EMPTY, segmentId, new NullProcessingContext());

    Search s = new SearchBuilder().all().includeKeys(true).includeValues(true).attribute("name").attribute("age")
        .attributeAscending("address").build();

    SearchResult<IndexQueryResult> context = idxManager.searchIndex(idx, s);

    IndexQueryResult res = context.getQueryResults().get(0);
    assertEquals(1, res.getSortAttributes().size());
    assertNull(res.getSortAttributes().get(0).getObjectValue());
  }

  @Test
  public void testNull() throws IndexException {
    String idx = getName();
    List<NVPair> attributes = new ArrayList<NVPair>();

    long segmentId = 1984L;

    attributes.add(new AbstractNVPair.StringNVPair("name", "foo"));
    attributes.add(new AbstractNVPair.IntNVPair("age", 54));
    idxManager.insert(idx, "p0", new ValueID(100), attributes, EMPTY, segmentId, new NullProcessingContext());

    attributes.clear();
    attributes.add(new AbstractNVPair.StringNVPair("name", "bar"));
    attributes.add(new AbstractNVPair.DateNVPair("now", new Date()));
    idxManager.insert(idx, "p1", new ValueID(200), attributes, EMPTY, 11l, new NullProcessingContext());

    Search s = new SearchBuilder().includeValues(true).isNull("name").build();
    SearchResult<IndexQueryResult> context = idxManager.searchIndex(idx, s);
    List<IndexQueryResult> res = context.getQueryResults();
    assertTrue(res.isEmpty());

    s = new SearchBuilder().isNull("now").includeValues(true).build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(1, res.size());
    NonGroupedQueryResult hit = (NonGroupedQueryResult) res.get(0);
    ValueID v = hit.getValue();
    assertEquals(new ValueID(100), v);

    s = new SearchBuilder().isNull("address").includeValues(true).build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(2, res.size());

    s = new SearchBuilder().isNull("age").includeValues(true).build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(1, res.size());

    hit = (NonGroupedQueryResult) res.get(0);
    v = hit.getValue();
    assertEquals(new ValueID(200), v);

    s = new SearchBuilder().includeValues(true).notNull("name").build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(2, res.size());

    s = new SearchBuilder().notNull("now").includeValues(true).build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(1, res.size());
    hit = (NonGroupedQueryResult) res.get(0);
    v = hit.getValue();
    assertEquals(new ValueID(200), v);

    s = new SearchBuilder().notNull("address").includeValues(true).build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(0, res.size());

    s = new SearchBuilder().notNull("age").includeValues(true).build();
    context = idxManager.searchIndex(idx, s);
    res = context.getQueryResults();
    assertEquals(1, res.size());

    hit = (NonGroupedQueryResult) res.get(0);
    v = hit.getValue();
    assertEquals(new ValueID(100), v);

  }
  // Comment out for now
  // public void testAggregatorUnknownAttribute() throws IndexException {
  // String name = getName();
  // int minVal = 100, count = 50;
  // for (int i = 0; i < count; i++) {
  // List<NVPair> attributes = new ArrayList<NVPair>();
  // attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));
  // idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, EMPTY, i, new NullProcessingContext());
  // }
  //
  // // test functions
  // Set<String> attributeSet = Collections.EMPTY_SET;
  // LinkedList queryStack = new LinkedList();
  // queryStack.addFirst(StackOperations.ALL);
  //
  // List<NVPair> aggregatorList = new ArrayList<NVPair>();
  // // Unknown attribute
  // aggregatorList.add(AbstractNVPair.createNVPair("height", AggregatorOperations.MAX));
  //
  // aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.COUNT));
  // aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MAX));
  // aggregatorList.add(AbstractNVPair.createNVPair("age", AggregatorOperations.MIN));
  //
  // SearchResult context = idxManager.searchIndex(name, queryStack, true, true, attributeSet, Collections.EMPTY_SET,
  // Collections.EMPTY_LIST, aggregatorList, -1);
  //
  // List<Aggregator> results = context.getAggregators();
  // assertEquals(3, results.size());
  // System.out.println("Count : " + results.get(0).getResult());
  // assertEquals(count, results.get(0).getResult());
  // System.out.println("Max : " + results.get(1).getResult());
  // assertEquals(149, results.get(1).getResult());
  // System.out.println("Min : " + results.get(2).getResult());
  // assertEquals(100, results.get(2).getResult());
  //
  // }

  @Test
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
    // Create a second count aggregator, for a good measure
    Search s = new SearchBuilder().all().includeKeys(true).includeValues(true).max("age").min("age").count().sum("age")
        .average("age").count().build();

    SearchResult context = idxManager.searchIndex(name, s);

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
    SearchBuilder sb = new SearchBuilder().includeKeys(true).includeValues(true).max("age").min("age").count()
        .sum("age").average("age").count().max("name");
    sb.beginGroup().or();
    for (int i = 1; i < count; i += 2) {
      sb.term("age", minVal + i);
    }
    sb.endGroup();

    context = idxManager.searchIndex(name, sb.build());
    results = context.getAggregators();
    assertEquals(7, results.size());
    assertNull(results.get(6).getResult());
  }

  @Test
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
    // Run size-limited search, with given sort order
    int limit = 4;
    int min = maxVal - limit + 1;

    Search s = new SearchBuilder().all().includeKeys(true).includeValues(true).count().max("age").min("age").sum("age")
        .average("age").attributeDescending("age").maxResults(limit).build();

    SearchResult context = idxManager.searchIndex(name, s);

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

  @Test
  public void testMaxResults() throws IndexException {
    String name = getName();
    int minVal = 100;
    int count = idxCt * 3;

    for (int i = 0; i < count; i++) {
      List<NVPair> attributes = new ArrayList<NVPair>();
      attributes.add(new AbstractNVPair.IntNVPair("age", minVal + i));

      idxManager.insert(name, "key" + i, new ValueID(i * 4), attributes, EMPTY, i, new NullProcessingContext());
    }

    Search s = new SearchBuilder().all().includeKeys(true).includeValues(true).attributeDescending("age").maxResults(2)
        .build();

    // top 2 results, from sorted list
    SearchResult context = idxManager.searchIndex(name, s);

    List<NonGroupedQueryResult> res = context.getQueryResults();
    System.out.println(res);
    assertEquals("key" + (count - 1), res.get(0).getKey());
    assertEquals("key" + (count - 2), res.get(1).getKey());
    assertEquals(2, res.size());

    // any 2 results - unsorted list
    context = idxManager.searchIndex(name, s);

    assertEquals(2, context.getQueryResults().size());

    // All results; max > result set
    s = new SearchBuilder().all().includeKeys(true).includeValues(true).attributeDescending("age")
        .maxResults(count + 1).build();
    context = idxManager.searchIndex(name, s);

    assertEquals(count, context.getQueryResults().size());

  }

  @Test
  public void testReplace() throws IndexException {
    String cacheName = "foo";
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    String key = "replace";

    idxManager.replace(cacheName, key, new ValueID(2), new ValueID(1), attributes, EMPTY, 0,
                       new NullProcessingContext());

    List<IndexQueryResult> res = searchResults(cacheName);
    assertEquals(0, res.size());
    idxManager.insert(cacheName, key, new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());
    res = searchResults(cacheName);
    assertEquals(1, res.size());

    for (int i = 0; i < 10; i++) {
      idxManager.replace(cacheName, key, new ValueID(i + 2), new ValueID(i + 1), attributes, EMPTY, 0,
                         new NullProcessingContext());
    }
    assertEquals(1, res.size());
  }

  @Test
  public void testIncompleteIndexGroup() throws IndexException, IOException {
    FileFilter dirsOnly = new FileFilter() {
      @Override
      public boolean accept(File path) {
        return path.isDirectory();
      }
    };

    List<NVPair> attributes = new ArrayList<NVPair>();
    // We need to initialize the index manager for these test as reinit will cause reupgrade
    idxManager.init();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    idxManager.insert("foo", "replace", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());
    assertEquals(idxCt, new File(getLuceneDir(), Util.sanitizeCacheName("foo")).listFiles(dirsOnly).length);

    verifyClean(dirsOnly);

    idxManager.init();

    verifyClean(dirsOnly);

    idxManager.shutdown();

    Random r = new Random();
    int removeIndex = r.nextInt(idxCt);

    Util.deleteDirectory(new File(getLuceneDir().getAbsolutePath() + File.separator + Util.sanitizeCacheName("foo"), Integer
        .toString(removeIndex)));

    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();
    assertEquals(0, getLuceneDir().listFiles(dirsOnly).length);

    idxManager.insert("xyz", "new value", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());

    verifyClean(dirsOnly);

    idxManager.shutdown();
    File init = new File(getLuceneDir().getAbsolutePath() + File.separator + Util.sanitizeCacheName("xyz") + File.separator
                         + Integer.toString(removeIndex), LuceneIndex.TERRACOTTA_INIT_FILE);
    boolean deleted = init.delete();
    assertTrue(init.getAbsolutePath(), deleted);

    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();

    assertEquals(0, getLuceneDir().listFiles(dirsOnly).length);
    idxManager.insert("xyz", "new value", new ValueID(1), attributes, EMPTY, 0, new NullProcessingContext());
    verifyClean(dirsOnly);

    idxManager.shutdown();
    File schema = new File(getLuceneDir().getAbsolutePath() + File.separator + Util.sanitizeCacheName("xyz"),
                           SearchConsts.TERRACOTTA_SCHEMA_FILE);
    assertTrue(schema.getAbsolutePath(), schema.canRead());
    assertTrue(schema.getPath(), schema.delete());

    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();

    assertEquals(0, getLuceneDir().listFiles(dirsOnly).length);

  }

  private void verifyClean(FileFilter dirsOnly) throws IOException {

    assertEquals(1, getLuceneDir().listFiles(dirsOnly).length);

    for (File dir : getLuceneDir().listFiles(dirsOnly)) {
      assertEquals(idxCt, dir.listFiles(dirsOnly).length);
      for (File subDir : dir.listFiles(dirsOnly)) {
        assertTrue(LuceneIndex.hasInitFile(subDir));
      }
    }
  }

  private List<IndexQueryResult> searchResults(String idxName) throws IndexException {
    Search s = new SearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").term("attr1", "foo")
        .build();
    SearchResult context = idxManager.searchIndex(idxName, s);
    return context.getQueryResults();
  }

  private File getLuceneDir(boolean clean) throws IOException {
    File dir = new File(getTempDirectory(), getName());
    if (clean) {
      Util.cleanDirectory(dir);
    }
    System.out.println(dir);
    return dir;
  }

  @Test
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
    SearchBuilder sb = new SearchBuilder().attribute("state").
        groupBy("state").groupBy("city").min("age").count().greaterThan("age", 3).attributeDescending("state");

    SearchResult<GroupedQueryResult> context = idxManager
        .searchIndex(name, sb.build());

    SortUtil.checkStringSortOrder(context.getQueryResults(), "state", true);
    for (GroupedQueryResult res : context.getQueryResults()) {
      assertEquals(1, res.getAttributes().size());
      assertEquals("state", res.getAttributes().get(0).getName());
      for (NVPair attr : res.getGroupedAttributes()) {
        System.out.print(attr.getName() + ": " + attr.getObjectValue() + "  ");
      }

      for (Aggregator agg : res.getAggregators()) {
        System.out.print(agg.getResult() + "  ");
      }
      System.out.println();
    }
  }

  @Test
  public void testAllTxnsAcked() throws Exception {
    final List<NVPair> storeOnlyattributes = new ArrayList<NVPair>();

    final ValueID valueOid = new ValueID(1);

    int docCt = idxCt * 10000;

    ExecutorService pool = Executors.newCachedThreadPool(new NamedThreadFactory("test-thread") {

      @Override
      public Thread newThread(Runnable r) {
        Thread t = super.newThread(r);
        t.setDaemon(true);
        return t;
      }

    });
    final int taskNum = idxCt * 4;
    Collection<Callable<Void>> writeTask = new ArrayList<Callable<Void>>();
    final CountDownLatch latch = new CountDownLatch(docCt);
    final Random rnd = new Random();
    for (int n = 0; n < taskNum; n++) {
      final int start = n, end = docCt + start;

      writeTask.add(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          for (int i = start; i < end; i += taskNum) {
            Thread.sleep(rnd.nextInt(10) + 1);
            List<NVPair> attributes = new ArrayList<NVPair>();

            attributes.add(new AbstractNVPair.StringNVPair("attr1", "FooBar-" + i));

            idxManager.insert("foo", "key-" + i, valueOid, attributes, storeOnlyattributes, i, new ProcessingContext() {
              @Override
              public void processed() {
                latch.countDown();
              }

            });
          }
          return null;
        }
      });
    }
    for (Future<Void> fut : pool.invokeAll(writeTask)) {
      fut.get();
    }

    if (!latch.await(30, TimeUnit.SECONDS)) fail("Didn't receive acks for " + latch.getCount() + " txns.");
  }


  @Test
  public void testLongCacheName() throws Exception {
    //Initialize before we start test as with every init we upgrade, we are already in upgraded state in this test
    idxManager.init();
    assertEquals(0, idxManager.getSearchIndexNames().length);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      sb.append('a');
    }
    String name = sb.toString();
    String name1 = name.substring(0, 450);
    String name2 = name.substring(0, 255);
    sb = new StringBuilder();
    for(int i = 0; i < 51; i++) {
      sb.append("%");
    }
    String escapedName = sb.toString();
    sb = new StringBuilder();
    for(int i =0; i < 253; i++) {
      sb.append("a");
    }
    sb.append("%");
    for(int i =0; i < 254; i++) {
      sb.append("a");
    }
    String escapedName1 = sb.toString();
    idxManager.insert(name, "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());
    idxManager.insert("foo","key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());
    idxManager.insert(name1, "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());
    idxManager.insert(name2, "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());
    idxManager.insert(escapedName, "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());
    idxManager.insert(escapedName1, "key", new ValueID(12), EMPTY, EMPTY, 34, new NullProcessingContext());

    //adding 5 caches and checking caches which have common parent path
    assertEquals(6, idxManager.getSearchIndexNames().length);
    List<String> indexNames = Arrays.asList(idxManager.getSearchIndexNames());
    //Checking presence of cache names
    assertTrue(indexNames.contains(name));
    assertTrue(indexNames.contains(name1));
    assertTrue(indexNames.contains(name2));
    assertTrue(indexNames.contains("foo"));
    assertTrue(indexNames.contains(escapedName));
    assertTrue(indexNames.contains(escapedName1));
    idxManager.shutdown();
    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();
    //adding 5 caches and checking caches which have common parent path
    assertEquals(6, idxManager.getSearchIndexNames().length);
    indexNames = Arrays.asList(idxManager.getSearchIndexNames());
    //Checking presence of cache names
    assertTrue(indexNames.contains(name));
    assertTrue(indexNames.contains(name1));
    assertTrue(indexNames.contains(name2));
    assertTrue(indexNames.contains("foo"));
    assertTrue(indexNames.contains(escapedName));
    assertTrue(indexNames.contains(escapedName1));
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

  @Test
  public void testDHL7480() throws IndexException, IOException {
    assertEquals(0, idxManager.getSearchIndexNames().length);
    List<NVPair> attributes = new ArrayList<NVPair>();

    attributes.add(new AbstractNVPair.StringNVPair("attr1", "FooBar"));

    List<NVPair> storeOnlyattributes = new ArrayList<NVPair>();
    storeOnlyattributes.add(new AbstractNVPair.ByteArrayNVPair("attr2", new byte[] { 6, 6, 6 }));

    ValueID valueOid = new ValueID(1);

    final int count = 50;
    // seed it.
    System.out.println("### Initial load of data: "+count+" elements.");
    for (int i = 0; i < count; i++) {
      idxManager.insert("foo", "key-" + i, valueOid, attributes, storeOnlyattributes, i, new NullProcessingContext());
    }

    // snapshot the index
    System.out.println("### Taking Query snapshot.");
    QueryID qid = new QueryID(1, 1);
    idxManager.snapshotIndex("foo", qid, new ProcessingContext() {
      @Override
      public void processed() {

      }
    });

    // thrash the data.
    System.out.println("### Mutating data.");
    for (int i = 0; i < count; i++) {
      int kindex = i % count;
      idxManager.update("foo", "key-" + kindex, new ValueID(i), attributes, storeOnlyattributes, i, new NullProcessingContext());
    }

    System.out.println("### Taking sync snapshot.");
    SyncSnapshot syncSnapshot = idxManager.snapshot("foo");

    List<IndexFile> indexFiles1 = syncSnapshot.getFilesToSync().get("foo");

    System.out.println("### Releasing sync snapshot.");
    idxManager.releaseResultSet("foo", qid, new ProcessingContext() {
      @Override
      public void processed() {

      }
    });

    System.out.println("### Checking disk file snapshot stability during mutation.");
    for (int i = 0; i < count * 10; i++) {
      int kindex = i % count;
      idxManager.update("foo", "key-" + kindex, new ValueID(i), attributes, storeOnlyattributes, i, new NullProcessingContext());
      if (kindex == 0) {
        checkFileExistences(indexFiles1);
        System.out.println("### Check: passed");
      }
    }
    System.out.println("### Disk files stable");
  }

  private void dumpFileExistences(List<IndexFile> indexFiles1) {
    System.out.println("Total files to sync" + indexFiles1.size());
    for (IndexFile file : indexFiles1) {
      boolean ok = true;
      try {
        InputStream open = idxManager.getIndexFile("foo", file.getIndexId(), file.getLuceneFilename());
        open.close();
      } catch (Exception e) {
        ok = false;
      }
      System.out.println(file.getDestFilename() + " :: " + ok);
    }
    System.out.println();
  }

  private void checkFileExistences(List<IndexFile> indexFiles1) {
    for (IndexFile file : indexFiles1) {
      try {
        InputStream open = idxManager.getIndexFile("foo", file.getIndexId(), file.getLuceneFilename());
        open.close();
      } catch (Exception e) {
        Assert.fail();
      }
    }
  }

}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.SearchBuilder.Search;
import com.terracottatech.search.aggregator.Aggregator;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;

public final class PagedSearchResultsTest extends SearchTestBase {
  private static final class TestIdxManager extends LuceneIndexManager {
    TestIdxManager(File indexDir, LoggerFactory loggerFactory, Configuration cfg) {
      super(indexDir, true, loggerFactory, cfg);
    }

    @Override
    public SearchResult searchIndex(String name, Search s) throws IndexException {
      if (s.getBatchSize() != Search.BATCH_SIZE_UNLIMITED)
        snapshotIndex(name, new QueryID(s.getRequesterId(), s.getQueryId()), new NullProcessingContext());
      return super.searchIndex(name, s);
    }

  }

  private final int MAX_CURSOR_LIMIT = 3;
  private final int MAX_BATCH_SIZE   = 10;

  public PagedSearchResultsTest() {
  }

  @Override
  protected LuceneIndexManager newIndexManager(File dir) {
    return new TestIdxManager(dir, loggerFactory, cfg);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    cfg.setDoAccessChecks(false);
    cfg.setMaxResultBatchSize(MAX_BATCH_SIZE);
    cfg.setMaxOpenResultSets(MAX_CURSOR_LIMIT);
  }

  @Test
  public void maxOpenResultsTest() throws Exception {
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    idxManager.searchMonitor.setInterval(1, TimeUnit.SECONDS);
    Search s = null;
    for (int i = 0; i < cfg.getMaxOpenResultSets(); i++) {
      s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
          .batchSize(cfg.getMaxResultBatchSize()).build();
      // test search
      SearchResult context = idxManager.searchIndex(getName(), s);
      assertEquals(dataSize, context.getTotalResultCount());
      assertEquals(cfg.getMaxResultBatchSize(), context.getQueryResults().size());
      System.out.println(idxManager.searchMonitor);
    }

    long lastSuccessQueryId = s.getQueryId();

    try {
      s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
          .batchSize(cfg.getMaxResultBatchSize()).build();
      // test search
      idxManager.searchIndex(getName(), s);
      fail("Expected exception but never got it");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

    idxManager.releaseResultSet(getName(), new QueryID(s.getRequesterId(), lastSuccessQueryId), new NullProcessingContext());

    s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
        .batchSize(cfg.getMaxResultBatchSize()).build();
    SearchResult context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(cfg.getMaxResultBatchSize(), context.getQueryResults().size());
  }

  @Test
  public void queryIdReuseTest() throws Exception {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = pageSize * 5;
    addData(dataSize);
    long clientId = 0, queryId = 10;
    Search s = new SearchBuilder().requesterId(clientId).queryId(queryId).includeKeys(true).attribute("attr1")
        .batchSize(pageSize).build();
    SearchResult res = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, res.getTotalResultCount());
    assertEquals(pageSize, res.getQueryResults().size());

    try {
      res = idxManager.searchIndex(getName(), s);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

  }

  @Test
  public void loadSmallResultsTest() throws Exception {
    int dataSize = cfg.getMaxResultBatchSize() - 1;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
        .batchSize(cfg.getMaxResultBatchSize()).build();
    // test search
    SearchResult context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(dataSize, context.getQueryResults().size());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

    // Allow repeated use of uncached query ids
    sendPagedProbeQuery(s.getRequesterId(), s.getQueryId(), dataSize, dataSize);
  }

  @Test
  public void loadCappedResultsTest() throws Exception {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = pageSize * 5;
    addData(dataSize);

    // Cap to single page size
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .maxResults(pageSize - 1).build();
    SearchResult context = idxManager.searchIndex(getName(), s);
    assertEquals(pageSize - 1, context.getTotalResultCount());
    assertEquals(pageSize - 1, context.getQueryResults().size());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

    // Allow repeated use of uncached query ids
    sendPagedProbeQuery(s.getRequesterId(), s.getQueryId(), pageSize, dataSize);
  }

  @Test
  public void loadUnpagedResultsTest() throws Exception {
    // Reset to regular index manager
    idxManager = new LuceneIndexManager(getLuceneDir(), true, loggerFactory, cfg);
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);

    Search s = new TestSearchBuilder().includeKeys(true).attribute("attr1").batchSize(Search.BATCH_SIZE_UNLIMITED)
        .build();
    SearchResult res = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, res.getTotalResultCount());
    assertEquals(dataSize, res.getQueryResults().size());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

  }

  @Test
  public void executeNoSnapshot() throws Exception {
    // Reset to regular index manager
    idxManager = new LuceneIndexManager(getLuceneDir(), true, loggerFactory, cfg);

    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
        .batchSize(pageSize).build();
    try {
      idxManager.searchIndex(getName(), s);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }
  }

  @Test
  public void loadReleasedResultsTest() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
        .batchSize(pageSize).build();
    // test search
    SearchResult context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    idxManager.releaseResultSet(getName(), new QueryID(s.getRequesterId(), s.getQueryId()), new NullProcessingContext());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

    // Allow reuse of query ids after result set is released
    sendPagedProbeQuery(s.getRequesterId(), s.getQueryId(), pageSize, dataSize);
  }

  @Test
  public void loadResultsAfterShutdown() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    idxManager.shutdown();

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 45, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

  }

  @Test
  public void loadResultsOnManagerRestart() throws Exception {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    ValueID valueOid = new ValueID(1);

    // Add extra data to index, just to make sure result set is frozen
    for (int i = dataSize; i < dataSize + 10; i++) {
      idxManager.insert(getName(), "key-" + i, valueOid, attributes, EMPTY, i, new NullProcessingContext());
    }
    context = idxManager.loadResultSet(getName(), s, 0, pageSize);

    List<String> keys = new ArrayList<String>(dataSize);
    for (NonGroupedQueryResult res : context.getQueryResults()) {
      keys.add(res.getKey());
    }

    idxManager.shutdown();

    idxManager = newIndexManager(getLuceneDir());
    idxManager.init();

    SearchResult<NonGroupedQueryResult> firstBatch = idxManager.loadResultSet(getName(), s, 0, pageSize);
    assertEquals(pageSize, firstBatch.getQueryResults().size());
    assertEquals(dataSize, firstBatch.getTotalResultCount());

    int n = 0;
    for (String key : keys.subList(0, pageSize)) {
      assertEquals(key, firstBatch.getQueryResults().get(n++).getKey());
    }

  }

  @Test
  public void loadResultsAfterFailover() throws Exception {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    ValueID valueOid = new ValueID(1);

    // Add extra data to index, just to make sure result set is frozen
    for (int i = dataSize; i < dataSize + 10; i++) {
      idxManager.insert(getName(), "key-" + i, valueOid, attributes, EMPTY, i, new NullProcessingContext());
    }

    context = idxManager.loadResultSet(getName(), s, 0, dataSize);

    List<String> keys = new ArrayList<String>(pageSize);
    for (NonGroupedQueryResult res : context.getQueryResults()) {
      keys.add(res.getKey());
    }

    String[] syncId = { "0", "1" };
    SyncSnapshot sync0 = idxManager.snapshot(syncId[0]);

    idxManager.insert(getName(), "key-" + (dataSize * 2), valueOid, attributes, EMPTY, 10, new NullProcessingContext());
    SyncSnapshot sync1 = idxManager.snapshot(syncId[1]);

    File destDir = new File(getTempDirectory(), getName() + "-passive");
    applySync(sync0, destDir);

    // Release sync and query result snapshots on original index dir, for good measure
    sync0.release();
    sync1.release();
    idxManager.releaseResultSet(getName(), new QueryID(s.getRequesterId(), s.getQueryId()), new NullProcessingContext());

    idxManager.shutdown();

    idxManager = newIndexManager(destDir);
    idxManager.init();

    SearchResult<NonGroupedQueryResult> all = idxManager.loadResultSet(getName(), s, 0, dataSize);
    assertEquals(pageSize, all.getQueryResults().size());
    assertEquals(dataSize, all.getTotalResultCount());

    int n = 0;
    for (String key : keys) {
      assertEquals(key, all.getQueryResults().get(n++).getKey());
    }
    idxManager.releaseResultSet(getName(), new QueryID(s.getRequesterId(), s.getQueryId()), new NullProcessingContext());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

    // shutdown() needs to be called before deleting files underneath; thus, can't rely on tearDown
    idxManager.shutdown();
    Util.deleteDirectory(destDir);
  }

  @Test
  public void failoverAfterRelease() throws Exception {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    idxManager.releaseResultSet(getName(), new QueryID(s.getRequesterId(), s.getQueryId()), new NullProcessingContext());

    String[] syncId = { "0", "1" };
    SyncSnapshot sync0 = idxManager.snapshot(syncId[0]);

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    ValueID valueOid = new ValueID(1);
    idxManager.insert(getName(), "key-" + (dataSize * 2), valueOid, attributes, EMPTY, 10, new NullProcessingContext());

    SyncSnapshot sync1 = idxManager.snapshot(syncId[1]);

    File destDir = new File(getTempDirectory(), getName() + "-passive");
    applySync(sync0, destDir);

    sync0.release();
    sync1.release();

    idxManager.shutdown();

    idxManager = newIndexManager(destDir);
    idxManager.init();

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, 100);
      fail("Expected exception");
    } catch (IndexException e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }

    // Allow reuse of query ids after result set is released
    sendPagedProbeQuery(s.getRequesterId(), s.getQueryId(), pageSize, dataSize);

    // shutdown() needs to be called before deleting files underneath; thus, can't rely on tearDown
    idxManager.shutdown();
    Util.deleteDirectory(destDir);
  }

  @Test
  public void repeatableResultLoading() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = pageSize * 5;

    // Intercept max page size
    cfg.setMaxResultBatchSize(dataSize);

    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));
    ValueID valueOid = new ValueID(1);

    // Add extra data to index, just to make sure result set is frozen
    for (int i = dataSize; i < dataSize + 10; i++) {
      idxManager.insert(getName(), "key-" + i, valueOid, attributes, EMPTY, i, new NullProcessingContext());
    }
    List<String> keys = new ArrayList<String>(dataSize);
    for (NonGroupedQueryResult res : context.getQueryResults()) {
      keys.add(res.getKey());
    }
    SearchResult<NonGroupedQueryResult> firstBatch = idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(),
                                                                              getName(), 0, pageSize);
    assertEquals(pageSize, firstBatch.getQueryResults().size());
    assertEquals(dataSize, firstBatch.getTotalResultCount());

    int n = 0;
    for (String key : keys.subList(0, pageSize)) {
      assertEquals(key, firstBatch.getQueryResults().get(n++).getKey());
    }

    // Load the rest of results in one shot
    SearchResult<NonGroupedQueryResult> rest = idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(),
                                                                        pageSize, dataSize /* okay to exceed end */);
    assertEquals(dataSize - pageSize, rest.getQueryResults().size());
    assertEquals(dataSize, rest.getTotalResultCount());

    for (NonGroupedQueryResult res : rest.getQueryResults()) {
      keys.add(res.getKey());
    }

    // Load in batches, verify sequencing
    for (int idx = pageSize; idx < dataSize; idx += pageSize) {
      SearchResult<NonGroupedQueryResult> batch = idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(),
                                                                           getName(), idx, pageSize);
      assertEquals(pageSize, batch.getQueryResults().size());
      assertEquals(dataSize, batch.getTotalResultCount());

      List<String> toCheck = keys.subList(idx, idx + pageSize);
      int i = 0;
      for (NonGroupedQueryResult batchRes : batch.getQueryResults()) {
        assertEquals(toCheck.get(i++), batchRes.getKey());
      }
    }
  }

  @Test
  public void aggregatorTest() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = 100;
    addData(dataSize);
    long start = 75, end = 97;
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).beginGroup().and()
        .between(LuceneIndex.VALUE_FIELD_NAME, start, LuceneIndex.VALUE_FIELD_NAME, end, true, true).isNull("notFound")
        .endGroup().count().average(LuceneIndex.VALUE_FIELD_NAME).batchSize(pageSize).build();
    double avg = (start + end) / 2;
    int count = (int) (end - start + 1); // inclusive

    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(count, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());
    assertTrue(context.isAnyCriteriaMatch());

    for (NonGroupedQueryResult res : context.getQueryResults()) {
      long val = res.getValue().toLong();
      assertTrue(String.valueOf(val), val <= end && val >= start);
    }
    verifyAggregators(context, count, avg);

    for (int idx = pageSize; idx < count; idx += pageSize) {
      SearchResult<NonGroupedQueryResult> batch = idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(),
                                                                           getName(), idx, pageSize);
      assertTrue(pageSize >= batch.getQueryResults().size());
      assertEquals(count, batch.getTotalResultCount());
      for (NonGroupedQueryResult res : batch.getQueryResults()) {
        long val = res.getValue().toLong();
        assertTrue(String.valueOf(val), val <= end && val >= start);
      }
      verifyAggregators(batch, count, avg);
    }
  }

  @Test
  public void stringOrderingTest() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = 100;
    addData(dataSize);
    long start = 35, end = 97;
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).beginGroup().and()
        .between(LuceneIndex.VALUE_FIELD_NAME, start, LuceneIndex.VALUE_FIELD_NAME, end, true, true).isNull("notFound")
        .endGroup().attributeAscending("attr1").batchSize(pageSize).build();
    int count = (int) (end - start + 1); // inclusive

    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(count, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());
    assertTrue(context.isAnyCriteriaMatch());

    List<NonGroupedQueryResult> all = new ArrayList<NonGroupedQueryResult>(count);

    SortUtil.checkStringSortOrder(context.getQueryResults(), "attr1", false);

    for (int idx = 0; idx < count; idx += pageSize) {
      SearchResult<NonGroupedQueryResult> batch = idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(),
                                                                           getName(), idx, pageSize);
      assertTrue(pageSize >= batch.getQueryResults().size());
      assertEquals(count, batch.getTotalResultCount());
      for (NonGroupedQueryResult res : batch.getQueryResults()) {
        long val = res.getValue().toLong();
        assertTrue(val <= end && val >= start);
      }
      all.addAll(batch.getQueryResults());
    }

    SortUtil.checkStringSortOrder(all, "attr1", false);
  }

  @Test
  public void groupByTest() throws IndexException {
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
    Search s = new SearchBuilder().attribute("state").groupBy("state").groupBy("city").greaterThan("age", 3)
        .attributeDescending("state").batchSize(3).build();

    SearchResult<GroupedQueryResult> context = idxManager.searchIndex(name, s);

    // All groups are in the one and only batch
    assertEquals(context.getTotalResultCount(), context.getQueryResults().size());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), name, 0, cfg.getMaxResultBatchSize());
      fail("Expected IndexException");
    } catch (IndexException e) {
      System.out.println("Caught expected exception " + e.getMessage());
    }
  }

  @Test
  public void batchLimitsTest() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = pageSize * 3;

    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    SearchResult<NonGroupedQueryResult> batch = idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(),
                                                                         pageSize, pageSize + 3);
    // Max is in effect
    assertEquals(pageSize, batch.getQueryResults().size());
    assertEquals(dataSize, batch.getTotalResultCount());

    try {
      idxManager.loadResultSet(s.getRequesterId(), s.getQueryId(), getName(), 0, Search.BATCH_SIZE_UNLIMITED);
      fail("Expected to get exception");
    } catch (Exception e) {
      System.err.println("Caught expected exception: " + e.getMessage());
    }
  }

  @Test
  public void indepedentResultLoadTest() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = pageSize * 5;

    cfg.setMaxResultBatchSize(dataSize);

    addData(dataSize);
    Search s1 = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    SearchResult<NonGroupedQueryResult> context = idxManager.searchIndex(getName(), s1);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    List<NVPair> attributes = new ArrayList<NVPair>();
    attributes.add(new AbstractNVPair.StringNVPair("attr1", "foo"));

    // Add extra page of data to index, just to make sure result set is frozen
    for (int i = dataSize; i < dataSize + pageSize; i++) {
      idxManager.insert(getName(), "key-" + i, new ValueID(i), attributes, EMPTY, i, new NullProcessingContext());
    }

    // Slurp in up to dataSize
    context = idxManager.loadResultSet(s1.getRequesterId(), s1.getQueryId(), getName(), 0, dataSize + 50);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(dataSize, context.getQueryResults().size());

    Search s2 = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1").batchSize(pageSize)
        .build();
    // test search
    context = idxManager.searchIndex(getName(), s2);
    assertEquals(dataSize + pageSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    context = idxManager.loadResultSet(s2.getRequesterId(), s2.getQueryId(), getName(), 0, dataSize + 50);
    assertEquals(dataSize + pageSize, context.getTotalResultCount());
    assertEquals(dataSize, context.getQueryResults().size()); // new size - pageSize
  }

  private void verifyAggregators(SearchResult<NonGroupedQueryResult> result, Object... value) {
    List<Aggregator> aggVals = result.getAggregators();
    int i = 0;
    for (Aggregator agg : aggVals) {
      assertEquals(value[i++], agg.getResult());
    }
  }

  private void sendPagedProbeQuery(long clientId, long queryId, int pageSize, int totalSize) throws IndexException {

    Search s = new SearchBuilder().requesterId(clientId).queryId(queryId).includeKeys(true).attribute("attr1")
        .batchSize(pageSize).build();
    SearchResult res = idxManager.searchIndex(getName(), s);
    assertEquals(totalSize, res.getTotalResultCount());
    assertEquals(pageSize, res.getQueryResults().size());

  }

  private void applySync(SyncSnapshot sync, File destDir) throws IOException {
    for (Map.Entry<String, List<IndexFile>> snap: sync.getFilesToSync().entrySet()) {
      String idxName = snap.getKey();
      for (IndexFile f: snap.getValue()) {
        File src = LuceneIndexManager.resolveIndexFilePath(getLuceneDir(), idxName, f.getIndexId(), f.getLuceneFilename());
        File dest = LuceneIndexManager.resolveIndexFilePath(destDir, idxName, f.getIndexId(), f.getDestFilename());
        Util.copyFile(src, dest);
      }
    }

  }

  @Test
  public void testSearchMonitorTest() throws IndexException {
    int pageSize = cfg.getMaxResultBatchSize();
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);
    Search s = new TestSearchBuilder().includeKeys(true).includeValues(true).attribute("attr1")
      .batchSize(pageSize).build();
    // test search
    idxManager.searchMonitor.setInterval(1, TimeUnit.SECONDS);
    SearchResult context = idxManager.searchIndex(getName(), s);
    assertEquals(dataSize, context.getTotalResultCount());
    assertEquals(pageSize, context.getQueryResults().size());

    System.out.println(idxManager.searchMonitor);

    Assert.assertThat(idxManager.searchMonitor.getLiveQuerySnapshot().size(), is(1));
    SearchMonitor.QueryInfo qi = idxManager.searchMonitor.getLiveQuerySnapshot().first();

    idxManager.releaseResultSet(getName(), new QueryID(s.getRequesterId(), s.getQueryId()), new NullProcessingContext());
    Assert.assertThat(idxManager.searchMonitor.getLiveQuerySnapshot().size(), is(0));


    // Allow reuse of query ids after result set is released
    sendPagedProbeQuery(s.getRequesterId(), s.getQueryId(), pageSize, dataSize);
  }

  @Test
  public void testRidiculousBatchedRace() throws IndexException, InterruptedException {
    int pageSize = 1;
    int dataSize = cfg.getMaxResultBatchSize() * 5;
    addData(dataSize);

    final AtomicBoolean tripped = new AtomicBoolean(false);
    final Semaphore semContinue = new Semaphore(1);
    semContinue.drainPermits();

    // this will pause before search is run to recreate this case.
    SearchResultsDisruptor.setPartitionedSearchGate(new Runnable() {
      @Override
      public void run() {
        try {
          tripped.set(true);
          semContinue.acquire(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    final Search s = new TestSearchBuilder().includeKeys(true)
      .includeValues(true)
      .attribute("attr1")
      .batchSize(1)
      .build();

    final String cacheName = getName();
    final AtomicBoolean failed = new AtomicBoolean(false);
    // this thread will remove the results before allowing search to proceed.
    // release should fail.
    Thread thread = new Thread() {
      @Override
      public void run() {
        while (!tripped.get()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
        try {
          idxManager.releaseResultSet(cacheName,
                                      new QueryID(s.getRequesterId(), s.getQueryId()),
                                      new NullProcessingContext());
        } catch (IndexException e) {
          failed.set(true);
        }
        semContinue.release();
      }
    };
    thread.start();

    // do search
    SearchResult context = idxManager.searchIndex(getName(), s);
    thread.join();

    Assert.assertFalse(failed.get());

    SearchResultsDisruptor.setPartitionedSearchGate(null);

  }
}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Constants;

import com.terracottatech.search.AbstractNVPair.EnumNVPair;
import com.terracottatech.search.aggregator.AbstractAggregator;
import com.terracottatech.search.aggregator.Aggregator;
import com.terracottatech.search.aggregator.Count;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LuceneIndexManager {

  private final Logger                                logger;

  private final AtomicBoolean                         init                       = new AtomicBoolean();
  private final ConcurrentMap<String, IndexGroup>     idxGroups                  = new ConcurrentHashMap<String, IndexGroup>();
  private final ConcurrentMap<String, Directory>      tempDirs                   = new ConcurrentHashMap<String, Directory>();
  private final ConcurrentMap<Directory, IndexOutput> tempRamOutput              = new ConcurrentHashMap<Directory, IndexOutput>();

  private final File                                  indexDir;
  private final boolean                               ramdir;
  private final boolean                               offHeapdir;
  private boolean                                     shutdown;
  private final boolean                               useCommitThread;

  private final int                                   perCacheIdxCt;
  private final ExecutorService                       queryThreadPool;

  private final LoggerFactory                         loggerFactory;
  private final Configuration                         config;

  static final String                                 TERRACOTTA_CACHE_NAME_FILE = "__terracotta_cache_name.txt";
  static final String                                 TERRACOTTA_SCHEMA_FILE     = "__terracotta_schema.properties";

  private static final String                         COUNT_AGG_NAME             = "COUNT";

  public LuceneIndexManager(File indexDir, boolean isPermStore, LoggerFactory loggerFactory, Configuration cfg) {
    this.loggerFactory = loggerFactory;
    this.indexDir = indexDir;
    this.perCacheIdxCt = cfg.indexesPerCahce();
    this.config = cfg;

    logger = loggerFactory.getLogger(getClass());
    logger.info("Lucene version: " + Constants.LUCENE_MAIN_VERSION);

    queryThreadPool = createQueryThreadPool(cfg.maxConcurrentQueries(), cfg.indexesPerCahce());

    boolean useRamdir = cfg.useRamDir();
    boolean useOffHeapdir = cfg.useOffHeap();
    if (useRamdir && useOffHeapdir) { throw new AssertionError(
                                                               "Can have both Ram Directory and OffHeap Directory enabled !"); }

    if (isPermStore && (useRamdir || useOffHeapdir)) logger
        .warn("Server persistence is configured for permanent store mode - ignoring ram directory setting.");

    ramdir = !isPermStore && useRamdir;
    offHeapdir = !isPermStore && useOffHeapdir;
    if (ramdir) {
      logger.warn("Using on-heap ram directory for search indices. Heap usage is unbounded");
    } else if (offHeapdir) {
      // if (offHeapStorageManager == null) { throw new AssertionError(
      // "OffHeap is not configured in tc-config.xml but offheap is enabled for search indexes. Please enabled offheap in Terracotta config.");
      // }
      logger.info("Using off-heap directory for search indices ");
    }
    useCommitThread = cfg.useCommitThread();
  }

  private static ExecutorService createQueryThreadPool(int maxConcurrentQueries, int indexesPerCahce) {
    // because each query can be against its own cache
    return Executors.newFixedThreadPool(maxConcurrentQueries * indexesPerCahce, new ThreadFactory() {
      private final AtomicInteger ct = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread worker = new Thread(r, "SearchQueryWorker-" + ct.incrementAndGet());
        worker.setDaemon(true);
        return worker;
      }
    });
  }

  public void init() throws IOException {
    if (init.compareAndSet(false, true)) {
      Util.ensureDirectory(indexDir);
      logger.info("Initializing lucene index directory at " + indexDir.getAbsolutePath() + " offheap : " + offHeapdir
                  + " ram : " + ramdir + " index/cache ratio : " + perCacheIdxCt);

      // load existing indices that might exist in the root dir
      Collection<File> incomplete = new ArrayList<File>();
      FileFilter dirsOnly = new FileFilter() {
        @Override
        public boolean accept(File path) {
          return path.isDirectory();
        }
      };

      for (File dir : indexDir.listFiles(dirsOnly)) {
        try {
          // We MUST use the original cache name for index group creation, for consistency with on-demand group
          // creation
          // during call chain from upsert()
          IndexGroup grp = getOrCreateGroup(loadName(dir), null);
          for (File subDir : dir.listFiles(dirsOnly)) {
            if (LuceneIndex.hasInitFile(subDir)) {
              grp.createIndex(Long.valueOf(subDir.getName()), true);
            } else {
              incomplete.add(subDir);
            }
          }
        } catch (IndexException e) {
          IOException ioe = new IOException(e);
          throw ioe;
        }
      }

      for (File dir : incomplete) {
        logger.warn("Removing incomplete index directory: " + dir.getAbsolutePath());
        Util.deleteDirectory(dir);
        if (dir.getParentFile().listFiles(dirsOnly).length == 0) Util.deleteDirectory(dir.getParentFile());
      }

      if (!tempDirs.isEmpty()) { throw new AssertionError("Not all temp ram/offheap dirs consumed: " + tempDirs); }
      if (!tempRamOutput.isEmpty()) { throw new AssertionError("Not all ram/offheap output closed: " + tempRamOutput); }
    }
  }

  public void optimizeSearchIndex(String indexName) {
    IndexGroup indexes = getGroup(indexName);
    if (indexes == null) {
      logger.warn("Ignoring request to optimize non-existent indexes [" + indexName + "]");
      return;
    }
    indexes.optimize();
  }

  public String[] getSearchIndexNames() {
    return indexDir.isDirectory() ? indexDir.list() : new String[0];
  }

  private IndexGroup getOrCreateGroup(String name, List<NVPair> attrs) throws IndexException {
    IndexGroup group = getGroup(name);
    if (group == null) {
      group = new IndexGroup(name);
      IndexGroup tmp = idxGroups.putIfAbsent(name, group);
      if (tmp != null) group = tmp;
      else {
        group.storeName();
        Map<String, ValueType> idxSchema;
        synchronized (group) {
          if (attrs == null) idxSchema = group.loadSchema();
          else {
            idxSchema = extractSchema(attrs);
            group.storeSchema(idxSchema);
          }
          group.schema.putAll(idxSchema);
        }
      }
    }
    return group;
  }

  private IndexGroup getGroup(String name) {
    return idxGroups.get(name);
  }

  public SearchResult searchIndex(String name, final List queryStack, final boolean includeKeys,
                                  final boolean includeValues, final Set<String> attributeSet,
                                  final List<NVPair> sortAttributes, final List<NVPair> aggregators,
                                  final int maxResults) throws IndexException {
    IndexGroup indexes = getGroup(name);
    if (indexes == null) {
      // Return empty set, since index might not exist on this stripe
      return SearchResult.NULL_RESULT;
    }

    return indexes.searchIndex(queryStack, includeKeys, includeValues, attributeSet, sortAttributes, aggregators,
                               maxResults);

  }

  public synchronized void shutdown() {
    if (shutdown) return;
    queryThreadPool.shutdown();
    shutdown = true;

    for (IndexGroup group : idxGroups.values()) {
      group.close();
    }
    idxGroups.clear();
  }

  private Directory directoryFor(String name, String idxId, File path) throws IOException {
    String dirName = getDirName(name, idxId);
    if (ramdir || offHeapdir) {
      Directory dir = tempDirs.remove(dirName);
      if (dir == null) { throw new AssertionError("missing ramdir/offheap dir for " + dirName + ": " + tempDirs); }
      return dir;
    } else {
      return FSDirectory.open(path);
    }
  }

  private Directory createDirectoryFor(File path) throws IOException {
    if (ramdir) {
      return new RAMDirectory();
    } else if (offHeapdir) {
      throw new AssertionError();
      // return new OffHeapDirectory(path.getPath(), offHeapStorageManager);
    } else {
      return FSDirectory.open(path);
    }
  }

  public void remove(String indexName, Object key, long segmentOid, ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.remove(key, segmentOid, context);
    } else {
      logger.warn("Remove failed: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void replace(String indexName, Object key, ValueID value, Object previousValue, List<NVPair> attributes,
                      long segmentOid, ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.replaceIfPresent(key, value, previousValue, attributes, segmentOid, context);
    } else {
      logger.warn("Replace failed: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void removeIfValueEqual(String indexName, Map<String, ValueID> toRemove, long segmentOid,
                                 ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.removeIfValueEqual(toRemove, segmentOid, context);
    } else {
      logger.warn("RemoveIfValueEqual failed: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void update(String indexName, Object key, ValueID value, List<NVPair> attributes, long segmentOid,
                     ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) group.update(key, value, attributes, segmentOid, context);
    else {
      logger.warn("Update failed: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void insert(String indexName, Object key, ValueID value, List<NVPair> attributes, long segmentOid,
                     ProcessingContext context) throws IndexException {
    IndexGroup group = getOrCreateGroup(indexName, attributes);
    group.insert(key, value, attributes, segmentOid, context);
  }

  public void clear(String indexName, long segmentOid, ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.clear(segmentOid, context);
    } else {
      logger.warn("Clear failed: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  private static Map<String, ValueType> extractSchema(List<NVPair> attributes) throws IndexException {
    Map<String, ValueType> schema = new HashMap<String, ValueType>();

    for (NVPair attr : attributes) {
      ValueType prev = schema.put(attr.getName(), attr.getType());
      if (prev != null && attr.getType() != prev) {
        //
        throw new IndexException("Differing types for repeated attribute: " + attr.getName());
      }
    }

    return schema;
  }

  public SyncSnapshot snapshot() throws IndexException {
    final Map<String, List<IndexFile>> filesToSync = getFilesToSync();

    return new SyncSnapshot() {
      public void release() {
        for (String name : filesToSync.keySet()) {
          LuceneIndexManager.this.release(name);
        }
      }

      public Map<String, List<IndexFile>> getFilesToSync() {
        return filesToSync;
      }
    };
  }

  public InputStream getIndexFile(String cacheName, String indexId, String fileName) throws IOException {
    IndexGroup group = getGroup(cacheName);
    if (group == null) { throw new AssertionError("missing index group for " + cacheName); }

    return group.getIndexFile(indexId, fileName);
  }

  private void release(String name) {
    IndexGroup group = getGroup(name);
    if (group != null) {
      group.release();
    } else {
      logger.error("No such index group [" + name + "] exists to release");
    }
  }

  private Map<String, List<IndexFile>> getFilesToSync() throws IndexException {
    Map<String, List<IndexFile>> filesSyncMap = new HashMap();
    for (Map.Entry<String, IndexGroup> entry : idxGroups.entrySet()) {

      String cacheName = entry.getKey();
      List<IndexFile> idxFiles = entry.getValue().getSyncFiles();
      if (idxFiles != null) filesSyncMap.put(cacheName, idxFiles);
    }
    return filesSyncMap;
  }

  public void applyIndexSync(String cacheName, String indexId, String fileName, byte[] fileData, boolean isTCFile,
                             boolean isLast) throws IOException {
    if (!ramdir && !offHeapdir || isTCFile) {
      File cacheIndexDir = new File(indexDir, Util.sanitizeCacheName(cacheName));
      if (indexId != null) cacheIndexDir = new File(cacheIndexDir, indexId);
      Util.ensureDirectory(cacheIndexDir);

      File syncFile = new File(cacheIndexDir, fileName);
      syncFile.createNewFile();
      FileOutputStream fos = new FileOutputStream(syncFile, true);
      try {
        fos.write(fileData);
        fos.flush();
      } finally {
        fos.close();
      }
    }

    if ((ramdir || offHeapdir) && !isTCFile) {
      Directory dir = getOrCreateTempRamDirectory(cacheName, indexId);

      IndexOutput output = tempRamOutput.get(dir);
      if (output == null) {
        output = dir.createOutput(fileName);
        tempRamOutput.put(dir, output);
      }

      output.writeBytes(fileData, fileData.length);

      if (isLast) {
        output.close();
        tempRamOutput.remove(dir);
      }
    }
  }

  private static String getDirName(String cacheName, String idxId) {
    return cacheName + File.separator + idxId;
  }

  private synchronized Directory getOrCreateTempRamDirectory(String cacheName, String idxId) {
    String indexName = getDirName(cacheName, idxId);
    Directory memoryDir = tempDirs.get(indexName);
    if (memoryDir != null) { return memoryDir; }
    if (ramdir) {
      memoryDir = new RAMDirectory();
    } else if (offHeapdir) {
      throw new AssertionError();
      // memoryDir = new OffHeapDirectory(indexName, offHeapStorageManager);
    } else {
      throw new AssertionError("Shouldnt get here");
    }
    Directory existing = tempDirs.put(indexName, memoryDir);
    if (existing != null) { throw new AssertionError("Directory for " + indexName + " already exists"); }
    return memoryDir;
  }

  final class IndexGroup {
    private final ConcurrentMap<Integer, LuceneIndex> indices = new ConcurrentHashMap<Integer, LuceneIndex>(
                                                                                                            perCacheIdxCt);
    private final String                              groupName;
    private final ConcurrentMap<String, ValueType>    schema  = new ConcurrentHashMap<String, ValueType>();
    private File                                      schemaSnapshot;

    private IndexGroup(String name) throws IndexException {
      groupName = name;
      try {
        Util.ensureDirectory(getPath());
      } catch (IOException x) {
        throw new IndexException(x);
      }
    }

    private void close() {
      for (LuceneIndex index : indices.values()) {
        index.close();
      }
    }

    private LuceneIndex getIndex(long segmentId) {
      return indices.get(getIndexId(segmentId));
    }

    private int getIndexId(long segmentOid) {
      return (int) (Math.abs(segmentOid) % perCacheIdxCt);
    }

    private InputStream getIndexFile(String indexId, String fileName) throws IOException {
      if (indexId != null) {
        Integer idxId;
        try {
          idxId = Integer.valueOf(indexId);
        } catch (NumberFormatException e) {
          throw new RuntimeException(String.format("Illegal index id %s", indexId), e);
        }

        LuceneIndex idx = indices.get(idxId);
        if (idx == null) throw new AssertionError(String.format("Non-existent index id %d specified for group %s",
                                                                idxId, groupName));
        return idx.getIndexFile(fileName);
      } else { // special TC-only file
        return new BufferedInputStream(new FileInputStream(new File(getPath(), fileName)));
      }
    }

    private synchronized void release() {
      if (schemaSnapshot != null && schemaSnapshot.exists()) {
        boolean deleted = schemaSnapshot.delete();
        if (!deleted) {
          logger.warn("failed to delete temp schema snapshot: " + schemaSnapshot.getAbsolutePath());
        }
        schemaSnapshot = null;
      }

      for (LuceneIndex index : indices.values())
        index.release();

    }

    private synchronized List<IndexFile> getSyncFiles() throws IndexException {
      List<IndexFile> files = new ArrayList<IndexFile>();
      // Add TC files first so they will exist before index opened on a passive syncing

      // XXX: when we support multiple concurrent snapshots this temp file
      // will need to be managed for each independent snapshot
      File schemaFile = new File(getPath(), TERRACOTTA_SCHEMA_FILE);
      if (schemaFile.exists()) {
        try {
          schemaSnapshot = File.createTempFile("tmp", TERRACOTTA_SCHEMA_FILE, getPath());
          Util.copyFile(schemaFile, schemaSnapshot);
        } catch (IOException e) {
          throw new IndexException(e);
        }
        files.add(new IndexFileImpl(TERRACOTTA_SCHEMA_FILE, schemaSnapshot.getName(), null, true, schemaSnapshot
            .length()));
      } else {
        logger.info("Schema file doesn't exist: " + schemaFile);
      }

      // include the cache name file (it never changes so need for temp copy)
      files.add(new IndexFileImpl(TERRACOTTA_CACHE_NAME_FILE, TERRACOTTA_CACHE_NAME_FILE, null, true,
                                  new File(getPath(), TERRACOTTA_CACHE_NAME_FILE).length()));

      for (LuceneIndex idx : indices.values()) {
        List<IndexFile> idxFiles = idx.getSnapshot();
        if (idxFiles != null) files.addAll(idxFiles);
      }
      return files;
    }

    private void optimize() {
      for (LuceneIndex index : indices.values())
        try {
          index.optimize();
        } catch (Exception e) {
          logger.error("Error optimizing index [" + groupName + "/" + index.getName() + "]", e);
        }

    }

    private void replaceIfPresent(Object key, ValueID value, Object previousValue, List<NVPair> attributes,
                                  long segmentOid, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentOid);
      if (index != null) {
        index.replaceIfPresent(key, value, previousValue, attributes, segmentOid, context);
      } else logger.warn(String.format("Unable to run replaceIfPresent: segment %s has no index in group %s",
                                       segmentOid, groupName));
    }

    private void removeIfValueEqual(Map<String, ValueID> toRemove, long segmentOid, ProcessingContext context)
        throws IndexException {
      LuceneIndex index = getIndex(segmentOid);
      if (index != null) {
        index.removeIfValueEqual(toRemove, context);
      } else logger.warn(String.format("Unable to run removeIfValueEqual: segment %s has no index in group %s",
                                       segmentOid, groupName));
    }

    private void remove(Object key, long segmentOid, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentOid);
      if (index != null) {
        index.remove(key, context);
      } else logger.warn(String.format("Unable to run remove: segment %s has no index in group %s", segmentOid,
                                       groupName));

    }

    private void clear(long segmentOid, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentOid);
      if (index != null) {
        index.clear(segmentOid, context);
      } else logger.warn(String.format("Unable to run clear: segment %s has no index in group %s", segmentOid,
                                       groupName));

    }

    private void update(Object key, ValueID value, List<NVPair> attributes, long segmentOid, ProcessingContext context)
        throws IndexException {
      LuceneIndex index = getIndex(segmentOid);
      if (index != null) {
        index.update(key, value, attributes, segmentOid, context);
      } else {
        logger.warn(String.format("Unable to run update: segment %s has no index in group %s", segmentOid, groupName));
      }

    }

    private void insert(Object key, ValueID value, List<NVPair> attributes, long segmentOid, ProcessingContext context)
        throws IndexException {
      LuceneIndex index = getIndex(segmentOid);
      if (index == null) {
        try {
          index = createIndex(segmentOid, false);
        } catch (IOException ioe) {
          throw new IndexException(ioe);
        }
      }

      index.insert(key, value, attributes, segmentOid, context);
    }

    private Map<String, Collection<Aggregator>> createAggregators(List<NVPair> requestedAggregators) {
      if (requestedAggregators.isEmpty()) { return Collections.EMPTY_MAP; }

      Map<String, Collection<Aggregator>> rv = new HashMap<String, Collection<Aggregator>>();
      NVPair count = null;
      for (NVPair aggregator : requestedAggregators) {
        String attrName = aggregator.getName();

        // Whoa this is ugly. The count aggregator NVPair is special in that it is not tied to any
        // attributes. Instead of returning the name of associated attribute, it simply returns COUNT for getName().
        // So create a separate entry in the map for it, and treat it specially in the caller method.
        if (attrName.equals(COUNT_AGG_NAME)) {
          count = aggregator;
          continue;
        }
        Collection<Aggregator> attrAggregators = rv.get(attrName);
        if (attrAggregators == null) {
          attrAggregators = new ArrayList<Aggregator>();
          rv.put(attrName, attrAggregators);
        }
        attrAggregators.add(createAggregator(aggregator));
      }
      if (count != null) rv.put(count.getName(), Collections.singletonList(createAggregator(count)));

      return rv;
    }

    private Aggregator createAggregator(NVPair aggregator) {
      EnumNVPair enumPair = (EnumNVPair) aggregator;
      String attributeName = enumPair.getName();

      // XXX: what if type doesn't exist in schema here?
      ValueType type = schema.get(attributeName);

      // XXX: do error checking and optimization here when decoding the enum ordinal
      AggregatorOperations aggregatorType = AggregatorOperations.values()[enumPair.getOrdinal()];
      return AbstractAggregator.aggregator(aggregatorType, attributeName, type);
    }

    private SearchResult searchIndex(final List queryStack, final boolean includeKeys, final boolean includeValues,
                                     final Set<String> attributeSet, final List<NVPair> sortAttributes,
                                     final List<NVPair> aggPairs, final int maxResults) throws IndexException {
      SearchResult mergeResult = new SearchResult(new ArrayList<IndexQueryResult>(), new ArrayList<Aggregator>(), false);

      Collection<Callable<SearchResult>> searchTasks = new ArrayList<Callable<SearchResult>>();

      final Map<String, Collection<Aggregator>> aggregators;
      try {
        aggregators = createAggregators(aggPairs);
      } catch (IllegalArgumentException e) {
        throw new IndexException(e);
      }

      final boolean includeCount = aggregators.containsKey(COUNT_AGG_NAME);
      // similar to DEV-7048: force aggregator attributes into result set, to correctly aggregate across all segments
      final Set<String> attrs = new HashSet<String>();
      attrs.addAll(attributeSet);
      for (String agg : aggregators.keySet()) {
        if (includeCount && COUNT_AGG_NAME.equals(agg)) continue;
        attrs.add(agg);
      }

      for (final LuceneIndex idx : indices.values()) {
        searchTasks.add(new Callable<SearchResult>() {
          public SearchResult call() throws Exception {
            return idx.search(queryStack, includeKeys, includeValues, attrs, sortAttributes, maxResults, includeCount);
          }
        });
      }

      boolean unOrdered = sortAttributes == null || sortAttributes.isEmpty();
      try {
        boolean isAny = mergeResult.isAnyCriteriaMatch();
        for (Future<SearchResult> fut : queryThreadPool.invokeAll(searchTasks)) {
          SearchResult curRes = fut.get();
          List<IndexQueryResult> qRes = mergeResult.getQueryResults();

          isAny |= curRes.isAnyCriteriaMatch();
          if (unOrdered && maxResults >= 0) {
            Iterator<IndexQueryResult> iter = curRes.getQueryResults().iterator();
            while (iter.hasNext() && qRes.size() < maxResults) {
              qRes.add(iter.next());
            }
            // stop merging if result limit reached - there is no sorting so any N will do
            if (qRes.size() == maxResults) break;
          }
          // grab everything - we will sort and/or trim results below
          else qRes.addAll(curRes.getQueryResults());
        }
        mergeResult = new SearchResult(mergeResult.getQueryResults(), mergeResult.getAggregators(), isAny);

      } catch (Exception ex) {
        logger.error(String.format("Search executor for index group %s threw exception: ", groupName), ex);
        throw new IndexException(ex);
      }

      List<IndexQueryResult> allQueryResults = mergeResult.getQueryResults();
      if (!unOrdered) {
        Collections.sort(allQueryResults, new QueryResultComparator(sortAttributes));

        if (maxResults >= 0 && allQueryResults.size() > maxResults) {
          List<IndexQueryResult> trimmed = new ArrayList<IndexQueryResult>(allQueryResults.subList(0, maxResults));
          allQueryResults.clear();
          allQueryResults.addAll(trimmed);
        }
      }

      if (includeCount) {
        Collection<Aggregator> countAggs = aggregators.get(COUNT_AGG_NAME);
        assert !countAggs.isEmpty();
        Count count = (Count) countAggs.iterator().next();
        count.increment(allQueryResults.size());

        // if no keys, values or any other attributes/aggregators to include, we are done!
        if (!includeKeys && !includeValues && attrs.isEmpty()) allQueryResults.clear();
      }

      if (!aggregators.isEmpty()) {
        for (ListIterator<IndexQueryResult> rowItr = allQueryResults.listIterator(); rowItr.hasNext();) {

          // This is much uglier than it has to be, because getAttributes() returns an unmodifiable collection
          IndexQueryResult row = rowItr.next();
          List<NVPair> rowAttrs = new ArrayList<NVPair>(row.getAttributes());

          for (Iterator<NVPair> itr = rowAttrs.iterator(); itr.hasNext();) {
            NVPair attr = itr.next();
            Collection<Aggregator> attrAggs = aggregators.get(attr.getName());
            if (attrAggs == null) continue;

            // Special case: if the attribute was not originally requested but instead added by us to enable
            // aggregation,
            // now remove it - unless
            // maxResults is set (see DEV-7048 for details)
            if (!attributeSet.contains(attr.getName()) && maxResults < 0) {
              itr.remove();
              if (rowAttrs.isEmpty() && !includeKeys && !includeValues) {
                rowItr.remove();
              } else {
                rowItr.set(new IndexQueryResultImpl(row.getKey(), row.getValue(), rowAttrs, row.getSortAttributes()));
              }
            }
            for (Aggregator agg : attrAggs) {
              try {
                // This reverse conversion to String is to prevent ClassCastException in
                // AbstractNVPair.createNVPair(String, Object, ValueType) when aggregator is serialized back inside
                // reply
                // msg
                agg.accept(ValueType.ENUM == attr.getType() ? AbstractNVPair.enumStorageString((EnumNVPair) attr)
                    : attr.getObjectValue());
              } catch (IllegalArgumentException e) {
                throw new IndexException(e);
              }
            }
          }
        }
      }

      for (Collection<Aggregator> attrAggs : aggregators.values())
        mergeResult.getAggregators().addAll(attrAggs);

      return mergeResult;

    }

    private File getPath() {
      return new File(indexDir, Util.sanitizeCacheName(groupName));
    }

    private LuceneIndex createIndex(long segmentOid, boolean load) throws IndexException, IOException {
      synchronized (LuceneIndexManager.this) {
        if (shutdown) throw new IndexException("Index manager shutdown");

        LuceneIndex index = null;
        if ((index = getIndex(segmentOid)) != null) { return index; }

        File groupPath = getPath();

        int idxSegment = getIndexId(segmentOid);
        String idxStr = String.valueOf(idxSegment);
        File path = new File(groupPath, idxStr);

        final LuceneIndex luceneIndex;

        if (!load) {
          logger.info(String.format("Creating search index [%s/%d]", groupName, idxSegment));
          luceneIndex = new LuceneIndex(createDirectoryFor(path), idxStr, path, useCommitThread, this, config,
                                        loggerFactory);
        } else {
          luceneIndex = new LuceneIndex(directoryFor(groupName, idxStr, path), idxStr, path, useCommitThread, this,
                                        config, loggerFactory); // FIXME
          logger.info(String.format("Opening existing search index [%s/%d]", groupName, idxSegment));
        }

        indices.put(idxSegment, luceneIndex);
        return luceneIndex;
      }
    }

    void checkSchema(List<NVPair> attributes) throws IndexException {
      for (NVPair nvpair : attributes) {
        String attrName = nvpair.getName();

        if (attrName.equals(LuceneIndex.KEY_FIELD_NAME) || attrName.equals(LuceneIndex.VALUE_FIELD_NAME)
            || attrName.equals(LuceneIndex.SEGMENT_OID_FIELD_NAME)) {
          // XXX: this assertion needs coverage at a higher level too (eg. ehcache level maybe?)
          throw new IndexException("Illegal attribute name present: " + attrName);
        }

        ValueType schemaType = schema.get(attrName);
        if (schemaType == null) {
          // a new attribute name -- update schema
          synchronized (this) {
            // check again since races allowed up until this point
            schemaType = schema.get(attrName);

            if (schemaType == null) {
              Map<String, ValueType> clone = new HashMap<String, ValueType>(schema);
              ValueType prev = clone.put(attrName, nvpair.getType());
              if (prev != null) { throw new AssertionError("replaced mapping for " + attrName); }

              // attempt disk update
              logger.info("Updating stored schema");
              storeSchema(clone);

              // disk update okay -- now change memory
              prev = schema.put(attrName, nvpair.getType());
              if (prev != null) { throw new AssertionError("replaced mapping for " + attrName); }

              schemaType = nvpair.getType();
            }
          }
        }

        if (!schemaType.equals(nvpair.getType())) {
          //
          throw new IndexException("Attribute type (" + nvpair.getType().name() + ") does not match schema type ("
                                   + schemaType.name() + ")");
        }
      }

    }

    Map<String, ValueType> getSchema() {
      return Collections.unmodifiableMap(schema);
    }

    private void storeSchema(Map<String, ValueType> schemaToStore) throws IndexException {
      File tmp;
      File path = getPath();
      try {
        tmp = File.createTempFile("tmp", TERRACOTTA_SCHEMA_FILE, path);
      } catch (IOException e) {
        throw new IndexException(e);
      }

      Properties props = new Properties();

      for (Map.Entry<String, ValueType> entry : schemaToStore.entrySet()) {
        props.setProperty(entry.getKey(), entry.getValue().name());
      }

      FileOutputStream fout = null;
      try {
        fout = new FileOutputStream(tmp);
        props.store(fout, null);
      } catch (IOException e) {
        throw new IndexException(e);
      } finally {
        try {
          if (fout != null) {
            fout.close();
          }
        } catch (IOException ioe) {
          logger.warn(ioe);
        }
      }

      File schemaFile = new File(path, TERRACOTTA_SCHEMA_FILE);

      if (schemaFile.exists()) {
        boolean deleted = schemaFile.delete();
        if (!deleted) { throw new IndexException("Cannot delete old schema file: " + schemaFile.getAbsolutePath()); }
      }

      boolean moved = tmp.renameTo(schemaFile);
      if (!moved) {
        //
        throw new IndexException("Failed to rename temp file [" + tmp.getAbsolutePath() + "] to ["
                                 + schemaFile.getAbsolutePath() + "]");
      }

    }

    private Map<String, ValueType> loadSchema() throws IndexException {
      File schemaFile = new File(getPath(), TERRACOTTA_SCHEMA_FILE);

      Properties data = new Properties();
      FileInputStream fin = null;
      try {
        fin = new FileInputStream(schemaFile);
        data.load(fin);
      } catch (IOException ioe) {
        throw new IndexException(ioe);
      } finally {
        if (fin != null) {
          try {
            fin.close();
          } catch (IOException ioe) {
            logger.warn(ioe);
          }
        }
      }

      Map<String, ValueType> res = new HashMap<String, ValueType>();

      for (Enumeration<String> i = (Enumeration<String>) data.propertyNames(); i.hasMoreElements();) {
        String key = i.nextElement();

        String typeName = data.getProperty(key);

        ValueType type;
        try {
          type = Enum.valueOf(ValueType.class, typeName);
        } catch (IllegalArgumentException iae) {
          throw new IndexException("No such type (" + typeName + ") for key " + key);
        }

        res.put(key, type);
      }
      return res;
    }

    private void storeName() throws IndexException {
      File file = new File(getPath(), TERRACOTTA_CACHE_NAME_FILE);

      FileOutputStream out = null;
      try {
        out = new FileOutputStream(file);

        for (char c : groupName.toCharArray()) {
          byte b1 = (byte) (0xff & (c >> 8));
          byte b2 = (byte) (0xff & c);

          out.write(b1);
          out.write(b2);
        }

        out.flush();
      } catch (IOException e) {
        throw new IndexException(e);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (IOException ioe) {
            logger.error("error closing " + file, ioe);
          }
        }
      }

    }
  }

  /**
   * Load "unsanitized" cache name
   */
  private static String loadName(File path) throws IndexException {
    FileInputStream in = null;
    try {
      in = new FileInputStream(new File(path, TERRACOTTA_CACHE_NAME_FILE));

      StringBuilder sb = new StringBuilder();
      int read;
      byte[] buf = new byte[2];
      while ((read = in.read(buf)) != -1) {
        if (read != 2) { throw new IOException("read " + read + " bytes"); }
        char c = (char) ((buf[0] << 8) | (buf[1] & 0xff));
        sb.append(c);
      }
      return sb.toString();
    } catch (IOException ioe) {
      throw new IndexException(ioe);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ioe) {
          //
        }
      }
    }
  }

}

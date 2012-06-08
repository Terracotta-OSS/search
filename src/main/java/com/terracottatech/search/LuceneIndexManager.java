/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Constants;

import com.terracottatech.offheapstore.filesystem.FileSystem;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.search.AbstractNVPair.EnumNVPair;
import com.terracottatech.search.aggregator.AbstractAggregator;
import com.terracottatech.search.aggregator.Aggregator;
import com.terracottatech.search.aggregator.Count;
import com.terracottatech.search.store.OffHeapDirectory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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
  private final LoggerFactory                         loggerFactory;
  private final Configuration                         cfg;
  private final int                                   perCacheIdxCt;
  private final int                                   offHeapFileSegmentCount;
  private final int                                   offHeapFileBlockSize;
  private final int                                   offHeapFileMaxPageSize;
  private FileSystem                                  offHeapFileSystem;
  private final ExecutorService                       queryThreadPool;

  static final String                                 TERRACOTTA_CACHE_NAME_FILE = "__terracotta_cache_name.txt";
  static final String                                 TERRACOTTA_SCHEMA_FILE     = "__terracotta_schema.properties";
  private static final String                         COUNT_AGG_NAME             = "__TC_AGG_COUNT"
                                                                                   + LuceneIndexManager.class
                                                                                       .hashCode();

  public LuceneIndexManager(File indexDir, boolean isPermStore, LoggerFactory loggerFactory, Configuration cfg) {
    this.loggerFactory = loggerFactory;
    this.cfg = cfg;
    this.logger = loggerFactory.getLogger(LuceneIndexManager.class);
    this.perCacheIdxCt = cfg.indexesPerCache();
    this.offHeapFileSegmentCount = cfg.getOffHeapFileSegmentCount();
    this.offHeapFileBlockSize = cfg.getOffHeapFileBlockSize();
    this.offHeapFileMaxPageSize = cfg.getOffHeapFileMaxPageSize();

    logger.info("Lucene version: " + Constants.LUCENE_MAIN_VERSION);

    queryThreadPool = createQueryThreadPool(cfg.maxConcurrentQueries(), cfg.indexesPerCache());

    this.indexDir = indexDir;

    boolean useRamdir = cfg.useRamDir();
    boolean useOffHeapdir = cfg.useOffHeap();
    if (useRamdir && useOffHeapdir) { throw new AssertionError(
                                                               "Can't have both Ram Directory and OffHeap Directory enabled !"); }

    if (isPermStore && (useRamdir || useOffHeapdir)) logger
        .warn("Server persistence is configured for permanent store mode - ignoring ram directory setting.");

    ramdir = !isPermStore && useRamdir;
    offHeapdir = !isPermStore && useOffHeapdir;
    if (ramdir) {
      logger.warn("Using on-heap ram directory for search indices. Heap usage is unbounded");
    }
    useCommitThread = cfg.useCommitThread();
  }

  public LuceneIndexManager(File indexDir, boolean isPermStore, LoggerFactory loggerFactory, Configuration cfg,
                            PageSource pageSource) {
    this(indexDir, isPermStore, loggerFactory, cfg);
    logger.info("Using off-heap directory for search indices ");
    this.offHeapFileSystem = new OffheapFileSystem(pageSource, offHeapFileBlockSize, offHeapFileMaxPageSize,
                                                   offHeapFileSegmentCount);
  }

  private static ExecutorService createQueryThreadPool(int maxConcurrentQueries, int indexesPerCahce) {
    return Executors.newFixedThreadPool(maxConcurrentQueries * indexesPerCahce, // because
                                        // each
                                        // query
                                        // can
                                        // be
                                        // against
                                        // its
                                        // own
                                        // cache
                                        new ThreadFactory() {
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
          boolean isCleanGroup = true;
          File[] subDirs = dir.listFiles(dirsOnly);
          if (subDirs.length != perCacheIdxCt) {
            incomplete.add(dir);
            isCleanGroup = false;
          } else {
            for (File subDir : subDirs) {
              if (!LuceneIndex.hasInitFile(subDir)) {
                incomplete.add(dir);
                isCleanGroup = false;
                break;
              }
            }
          }
          if (isCleanGroup) getOrCreateGroup(loadName(dir), null, true);
        } catch (IndexException e) {
          IOException ioe = new IOException(e);
          throw ioe;
        }
      }

      for (File dir : incomplete) {
        logger.warn("Removing incomplete index directory: " + dir.getAbsolutePath());
        Util.deleteDirectory(dir);
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

  private IndexGroup getOrCreateGroup(String name, List<NVPair> attrs, boolean load) throws IndexException {
    IndexGroup group = getGroup(name);
    if (group == null) {
      synchronized (idxGroups) {
        group = getGroup(name);
        // double check for not creating indices again
        if (group == null) {
          group = new IndexGroup(name, load);
          IndexGroup prev = this.idxGroups.put(name, group);
          if (prev != null) { throw new AssertionError("replaced group for " + name); }
        }
      }
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
    return group;
  }

  private IndexGroup getGroup(String name) {
    return idxGroups.get(name);
  }

  public SearchResult searchIndex(String name, final List queryStack, final boolean includeKeys,
                                  final boolean includeValues, final Set<String> attributeSet,
                                  Set<String> groupByAttributes, final List<NVPair> sortAttributes,
                                  final List<NVPair> aggregators, final int maxResults) throws IndexException {
    IndexGroup indexes = getGroup(name);
    if (indexes == null) {
      // Return empty set, since index might not exist on this stripe
      return SearchResult.NULL_RESULT;
    }

    return indexes.searchIndex(queryStack, includeKeys, includeValues, attributeSet, groupByAttributes, sortAttributes,
                               aggregators, maxResults);

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

  private Directory createDirectoryFor(File path, String name) throws IOException {
    if (ramdir) {
      return new RAMDirectory();
    } else if (offHeapdir) {
      Random random = new Random();
      return new OffHeapDirectory(offHeapFileSystem, String.valueOf(random.nextInt()));
    } else {
      return FSDirectory.open(path);
    }
  }

  public void remove(String indexName, String key, long segmentId, ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.remove(key, segmentId, context);
    } else {
      logger.info("Remove ignored: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void replace(String indexName, String key, ValueID value, ValueID previousValue, List<NVPair> attributes,
                      List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context)
      throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.replaceIfPresent(key, value, previousValue, attributes, storeOnlyAttributes, segmentId, context);
    } else {
      logger.info("Replace ignored: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void removeIfValueEqual(String indexName, Map<String, ValueID> toRemove, long segmentId,
                                 ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.removeIfValueEqual(toRemove, segmentId, context);
    } else {
      logger.info("RemoveIfValueEqual ignored: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void update(String indexName, String key, ValueID value, List<NVPair> attributes,
                     List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context) throws IndexException {
    // Get or create here b/c draining the journal will do a blind update due to a possible race with index sync
    IndexGroup group = getOrCreateGroup(indexName, attributes, false);
    group.update(key, value, attributes, storeOnlyAttributes, segmentId, context);
  }

  public void insert(String indexName, String key, ValueID value, List<NVPair> attributes,
                     List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context) throws IndexException {
    IndexGroup group = getOrCreateGroup(indexName, attributes, false);
    group.insert(key, value, attributes, storeOnlyAttributes, segmentId, context);
  }

  public void updateKey(String indexName, String existingKey, String newKey, int segmentId, ProcessingContext context)
      throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.updateKey(existingKey, newKey, segmentId, context);
    } else {
      context.processed();
      throw new IndexException("No such group [" + indexName + "] exists");
    }
  }

  public void clear(String indexName, long segmentId, ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.clear(segmentId, context);
    } else {
      logger.info("Clear ignored: no such index group [" + indexName + "] exists");
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

  private synchronized Directory getOrCreateTempRamDirectory(String cacheName, String idxId) throws IOException {
    String indexName = getDirName(cacheName, idxId);
    Directory memoryDir = tempDirs.get(indexName);
    if (memoryDir != null) { return memoryDir; }
    if (ramdir) {
      memoryDir = new RAMDirectory();
    } else if (offHeapdir) {
      Random random = new Random();
      memoryDir = new OffHeapDirectory(offHeapFileSystem, String.valueOf(random.nextInt()));
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

    private IndexGroup(String name, boolean load) throws IndexException {
      groupName = name;

      // always set the type for our internal fields
      schema.put(LuceneIndex.KEY_FIELD_NAME, ValueType.STRING);
      schema.put(LuceneIndex.KEY_BYTES_FIELD_NAME, ValueType.BYTE_ARRAY);
      schema.put(LuceneIndex.VALUE_FIELD_NAME, ValueType.LONG);
      schema.put(LuceneIndex.SEGMENT_ID_FIELD_NAME, ValueType.LONG);

      try {
        Util.ensureDirectory(getPath());
        createIndices(load);
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

    private int getIndexId(long segmentId) {
      return (int) (Math.abs(segmentId) % perCacheIdxCt);
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

    private void replaceIfPresent(String key, ValueID value, ValueID previousValue, List<NVPair> attributes,
                                  List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context)
        throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run replaceIfPresent: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.replaceIfPresent(key, value, previousValue, attributes, storeOnlyAttributes, segmentId, context);
    }

    private void removeIfValueEqual(Map<String, ValueID> toRemove, long segmentId, ProcessingContext context)
        throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run removeIfValueEqual: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.removeIfValueEqual(toRemove, context);
    }

    private void remove(String key, long segmentId, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run remove: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.remove(key, context);

    }

    private void clear(long segmentId, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run clear: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.clear(segmentId, context);
    }

    private void update(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                        long segmentId, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run update: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.update(key, value, attributes, storeOnlyAttributes, segmentId, context);
    }

    private void insert(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                        long segmentId, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run insert: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.insert(key, value, attributes, storeOnlyAttributes, segmentId, context);
    }

    public void updateKey(String existingKey, String newKey, int segmentId, ProcessingContext context)
        throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run updateKey: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.updateKey(existingKey, newKey, segmentId, context);
    }

    private Map<String, List<Aggregator>> createAggregators(List<NVPair> requestedAggregators) {
      if (requestedAggregators.isEmpty()) { return Collections.EMPTY_MAP; }

      Map<String, List<Aggregator>> rv = new HashMap<String, List<Aggregator>>();
      NVPair count = null;
      for (NVPair aggregator : requestedAggregators) {
        String attrName = aggregator.getName();

        // Whoa this is ugly. The count aggregator NVPair is special in that it is not tied to any
        // attributes. Instead of returning the name of associated attribute, it simply returns COUNT for getName().
        // So create a separate entry in the map for it, and treat it specially in the caller method.
        if (AggregatorOperations.COUNT.equals(aggregator.getObjectValue())) {
          count = aggregator;
          continue;
        }
        List<Aggregator> attrAggregators = rv.get(attrName);
        if (attrAggregators == null) {
          attrAggregators = new ArrayList<Aggregator>();
          rv.put(attrName, attrAggregators);
        }
        attrAggregators.add(createAggregator(aggregator));
      }
      if (count != null) rv.put(COUNT_AGG_NAME, Collections.singletonList(createAggregator(count)));

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
                                     final Set<String> attributeSet, final Set<String> groupByAttributes,
                                     final List<NVPair> sortAttributes, final List<NVPair> aggPairs,
                                     final int maxResults) throws IndexException {
      SearchResult mergeResult = new SearchResult(new ArrayList<IndexQueryResult>(), new ArrayList<Aggregator>(), false);
      boolean isGroupBy = !groupByAttributes.isEmpty();
      Collection<Callable<SearchResult>> searchTasks = new ArrayList<Callable<SearchResult>>();

      final Map<String, List<Aggregator>> aggregators;
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
            return idx.search(queryStack, includeKeys, includeValues, attrs, groupByAttributes, sortAttributes,
                              maxResults, includeCount);
          }
        });
      }

      boolean unOrdered = sortAttributes == null || sortAttributes.isEmpty();
      List<? extends IndexQueryResult> allQueryResults = new ArrayList<IndexQueryResult>();
      try {
        boolean isAny = mergeResult.isAnyCriteriaMatch();
        for (Future<SearchResult> fut : queryThreadPool.invokeAll(searchTasks)) {
          SearchResult curRes = fut.get();

          isAny |= curRes.isAnyCriteriaMatch();
          if (unOrdered && !isGroupBy) {
            List<IndexQueryResult> qRes = mergeResult.getQueryResults();
            if (maxResults >= 0) {
              Iterator<IndexQueryResult> iter = curRes.getQueryResults().iterator();
              while (iter.hasNext() && qRes.size() < maxResults) {
                qRes.add(iter.next());
              }
              // stop merging if result limit reached - there is no sorting so any N will do
              if (qRes.size() == maxResults) break;
            }
            // grab everything
            else qRes.addAll(curRes.getQueryResults());
            mergeResult = new SearchResult(mergeResult.getQueryResults(), mergeResult.getAggregators(), isAny);
          }
          // grab everything - we will sort and/or trim results below
          else allQueryResults.addAll(curRes.getQueryResults());
        }

      } catch (Throwable t) {
        if (t instanceof ExecutionException) {
          t = t.getCause();
        }
        logger.error(String.format("Search executor for index group %s threw exception: ", groupName), t);

        if (t instanceof IndexException) { throw (IndexException) t; }
        throw new IndexException(t);
      }

      if (!unOrdered || isGroupBy) {

        if (isGroupBy) {
          // merge groups and sort (if needed) in one shot
          allQueryResults = mergeGroups(allQueryResults, sortAttributes, aggregators, attributeSet);
          mergeResult = new SearchResult(mergeResult.getQueryResults(), mergeResult.getAggregators(),
                                         !allQueryResults.isEmpty());
        } else {
          // XXX Can't use merge sort - for now. Lucene's internal sort treats missing numeric fields as zeros, and
          // strings are moved to the end.
          // allQueryResults = mergeSort(stripeResults, sortAttributes);
          Collections.sort(allQueryResults, new QueryResultComparator(sortAttributes));

          // Do not limit results for grouped searches b/c it's the client's job only! Doing the opposite with unordered
          // searches can erroneously
          // omit groups from this stripe
          if (maxResults >= 0 && allQueryResults.size() > maxResults) {
            List<IndexQueryResult> trimmed = new ArrayList<IndexQueryResult>(allQueryResults.subList(0, maxResults));
            allQueryResults = trimmed;
          }
        }
        mergeResult.getQueryResults().addAll(allQueryResults);
      }

      if (!isGroupBy) {
        allQueryResults = mergeResult.getQueryResults();

        Count count = null;
        if (includeCount) {
          List<Aggregator> countAggs = aggregators.remove(COUNT_AGG_NAME); // 1 element collection
          if (countAggs.isEmpty()) throw new AssertionError("Count aggregators: expected non-empty singleton list");
          count = (Count) countAggs.iterator().next();
          count.increment(allQueryResults.size());

          // if no keys, values or any other attributes/aggregators to include, we are done!
          if (!includeKeys && !includeValues && attrs.isEmpty()) allQueryResults.clear();
        }

        if (!aggregators.isEmpty()) {
          for (ListIterator<NonGroupedQueryResult> rowItr = (ListIterator<NonGroupedQueryResult>) allQueryResults
              .listIterator(); rowItr.hasNext();) {

            // This is much uglier than it has to be, because getAttributes() returns an unmodifiable collection
            NonGroupedQueryResult row = rowItr.next();
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
                } else rowItr.set(new NonGroupedIndexQueryResultImpl(row.getKey(), row.getValue(), rowAttrs, row
                    .getSortAttributes()));
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

        // XXX: Original order of aggregators not preserved here, but maybe it's okay
        if (count != null) mergeResult.getAggregators().add(count);
        for (Collection<Aggregator> attrAggs : aggregators.values())
          mergeResult.getAggregators().addAll(attrAggs);
      }
      if (!aggPairs.isEmpty()) reorderAggregators(aggPairs, isGroupBy, mergeResult);
      return mergeResult;

    }

    /**
     * Outbound aggregators no longer preserve original order, so make things right here
     */
    private void reorderAggregators(List<NVPair> requestedAggs, boolean isGroupBy,
                                    SearchResult<? extends IndexQueryResult> result) {
      Map<Set<String>, Integer> aggPositions = new HashMap<Set<String>, Integer>();

      int n = 0, ctCt = 0;

      for (NVPair reqAgg : requestedAggs) {
        Set<String> key = new HashSet<String>(2);
        String name = reqAgg.getName();

        // Make count aggregator's name unique to enable multiple copies (if that's what the caller wants)
        if (AggregatorOperations.COUNT.equals(reqAgg.getObjectValue())) {
          name = COUNT_AGG_NAME + ctCt++;
        }
        key.add(name);
        key.add(reqAgg.getObjectValue().toString()); // AggregatorOperations.toString()
        Integer prev = aggPositions.put(key, n++);
        if (prev != null) throw new AssertionError(String.format("Previous index mapping found for %s: %d", key, prev));
      }

      if (!isGroupBy) {
        alignAggregators(aggPositions, result.getAggregators(), n, ctCt);
      }

      else {
        for (GroupedQueryResult group : (List<GroupedQueryResult>) result.getQueryResults()) {
          alignAggregators(aggPositions, group.getAggregators(), n, ctCt);
        }
      }
    }

    private void alignAggregators(Map<Set<String>, Integer> aggIndices, List<Aggregator> target, int totalAggCount,
                                  int countOfCounts) {
      // Do not use target.size() here! Non-grouped search only creates one instance of count aggregator.
      Aggregator dest[] = new Aggregator[totalAggCount];
      for (Aggregator a : target) {
        AbstractAggregator agg = (AbstractAggregator) a;
        Set<String> id = new HashSet<String>();

        if (AggregatorOperations.COUNT.equals(agg.getOperation())) {

          for (int i = 0; i < countOfCounts; i++) {
            id.add(COUNT_AGG_NAME + i);
            id.add(agg.getOperation().toString());
            int idx = aggIndices.get(id);
            id.clear();
            dest[idx] = agg;
          }
          continue;
        }

        id.add(agg.getAttributeName());
        id.add(agg.getOperation().toString());
        int idx = aggIndices.get(id);
        id.clear();
        dest[idx] = agg;
      }
      target.clear();
      target.addAll(Arrays.asList(dest));

    }

    private void fillInAggregators(GroupedQueryResult result, Map<String, List<Aggregator>> aggs) {
      List<Aggregator> dest = result.getAggregators();
      Collection<Aggregator> countAggs = aggs.get(COUNT_AGG_NAME); // 1 element collection
      if (countAggs != null) {
        if (countAggs.isEmpty()) throw new AssertionError("Count aggregator: expected non-empty singleton list");
        Count count = (Count) countAggs.iterator().next();
        Count clone = new Count(count.getAttributeName(), count.getType());
        clone.accept(result);
        dest.add(clone);
      }

      for (NVPair attr : result.getAttributes()) {
        // Hmm, what if there's an actual attribute with the same name
        if (COUNT_AGG_NAME.equals(attr.getName())) continue;
        List<Aggregator> attrAggs = aggs.get(attr.getName());
        if (attrAggs == null) continue;

        for (Aggregator agg : attrAggs) {
          AbstractAggregator srcAgg = (AbstractAggregator) agg;
          Aggregator destAgg = AbstractAggregator.aggregator(srcAgg.getOperation(), srcAgg.getAttributeName(),
                                                             srcAgg.getType());

          // This reverse conversion to String is to prevent ClassCastException in
          // AbstractNVPair.createNVPair(String, Object, ValueType) when aggregator is serialized back inside reply
          // msg
          destAgg.accept(ValueType.ENUM == attr.getType() ? AbstractNVPair.enumStorageString((EnumNVPair) attr) : attr
              .getObjectValue());
          dest.add(destAgg);
        }
      }
    }

    private List<GroupedQueryResult> mergeGroups(List<? extends IndexQueryResult> stripeResults,
                                                 List<NVPair> sortAttributes,
                                                 Map<String, List<Aggregator>> aggregators,
                                                 Set<String> requestedAttributes) {
      Map<Set<NVPair>, GroupedQueryResult> uniqueGroups = new HashMap<Set<NVPair>, GroupedQueryResult>();

      for (IndexQueryResult stripeRes : stripeResults) {

        GroupedQueryResult group = (GroupedQueryResult) stripeRes;
        Set<NVPair> groupBy = group.getGroupedAttributes();
        fillInAggregators(group, aggregators);

        GroupedQueryResult dest = uniqueGroups.get(groupBy);
        if (dest == null) {
          uniqueGroups.put(groupBy, group);
        } else ResultTools.aggregate(dest.getAggregators(), group.getAggregators());
      }
      List<GroupedQueryResult> groups = new ArrayList<GroupedQueryResult>(uniqueGroups.values());

      // Special case: if the attribute was not originally requested but instead added by us to enable
      // aggregation, now remove it
      for (ListIterator<GroupedQueryResult> rowItr = groups.listIterator(); rowItr.hasNext();) {
        GroupedQueryResult res = rowItr.next();
        List<NVPair> rowAttrs = new ArrayList<NVPair>(res.getAttributes());
        for (Iterator<NVPair> itr = rowAttrs.iterator(); itr.hasNext();) {
          if (!requestedAttributes.contains(itr.next().getName())) {
            itr.remove();

            rowItr.set(new GroupedIndexQueryResultImpl(rowAttrs, res.getSortAttributes(), res.getGroupedAttributes(),
                                                       res.getAggregators()));
          }
        }
      }

      if (!sortAttributes.isEmpty()) Collections.sort(groups, new QueryResultComparator(sortAttributes));
      return groups;
    }

    private File getPath() {
      return new File(indexDir, Util.sanitizeCacheName(groupName));
    }

    private void createIndices(boolean load) throws IndexException, IOException {
      synchronized (LuceneIndexManager.this) {
        if (shutdown) throw new IndexException("Index manager shutdown");
        if (!this.indices.isEmpty()) { throw new AssertionError("not empty: " + this.indices); }

        File groupPath = getPath();
        for (int idxSegment = 0; idxSegment < perCacheIdxCt; idxSegment++) {
          String idxStr = String.valueOf(idxSegment);
          File path = new File(groupPath, idxStr);

          final LuceneIndex luceneIndex;

          if (!load) {
            logger.info(String.format("Creating search index [%s/%d]", groupName, idxSegment));
            luceneIndex = new LuceneIndex(createDirectoryFor(path, groupName), idxStr, path, useCommitThread, this,
                                          cfg, loggerFactory);
          } else {
            luceneIndex = new LuceneIndex(directoryFor(groupName, idxStr, path), idxStr, path, useCommitThread, this,
                                          cfg, loggerFactory); // FIXME
            logger.info(String.format("Opening existing search index [%s/%d]", groupName, idxSegment));
          }
          indices.put(idxSegment, luceneIndex);
        }

      }
    }

    void checkSchema(List<NVPair> attributes) throws IndexException {
      for (NVPair nvpair : attributes) {
        String attrName = nvpair.getName();

        if (attrName.equals(LuceneIndex.KEY_FIELD_NAME) || attrName.equals(LuceneIndex.VALUE_FIELD_NAME)
            || attrName.equals(LuceneIndex.SEGMENT_ID_FIELD_NAME)) {
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

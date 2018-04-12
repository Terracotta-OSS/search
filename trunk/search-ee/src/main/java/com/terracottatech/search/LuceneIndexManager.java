/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;

import com.terracottatech.offheapstore.filesystem.FileSystem;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import org.terracotta.offheapstore.paging.PageSource;
import com.terracottatech.search.SearchBuilder.Search;
import com.terracottatech.search.store.OffHeapDirectory;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.terracottatech.search.SearchConsts.TERRACOTTA_CACHE_NAME_FILE;
import static com.terracottatech.search.SearchConsts.TERRACOTTA_SCHEMA_FILE;
import static com.terracottatech.search.SearchConsts.TERRACOTTA_HASH_FILE;

public class LuceneIndexManager {

  private final Logger                                logger;
  private final AtomicBoolean                         init                       = new AtomicBoolean();
  private final ConcurrentMap<String, IndexGroup>     idxGroups                  = new ConcurrentHashMap<String, IndexGroup>();
  private final ConcurrentMap<String, Directory>      tempDirs                   = new ConcurrentHashMap<String, Directory>();
  private final ConcurrentMap<Directory, IndexOutput> tempRamOutput              = new ConcurrentHashMap<Directory, IndexOutput>();

  private final File                                  indexDir;
  private final boolean                               ramdir;
  private final boolean                               offHeapdir;
  private final LoggerFactory                         loggerFactory;
  private final Configuration                         cfg;
  private final int                                   perCacheIdxCt;
  private final ExecutorService                       queryThreadPool;
  private final FileSystem                            offHeapFileSystem;
  private boolean                                     shutdown;

  private final FileFilter                            dirsOnly = new FileFilter() {
                                                                  @Override
                                                                  public boolean accept(File path) {
                                                                    return path.isDirectory();
                                                                  }
                                                                };
  final SearchMonitor                                 searchMonitor;

  public LuceneIndexManager(File indexDir, boolean isPermStore, LoggerFactory loggerFactory, Configuration cfg) {
    this(indexDir, isPermStore, loggerFactory, cfg, null);
  }

  public LuceneIndexManager(File indexDir, boolean isPermStore, LoggerFactory loggerFactory, Configuration cfg,
                            PageSource pageSource) {
    this.loggerFactory = loggerFactory;
    this.searchMonitor=new SearchMonitor(loggerFactory);
    this.cfg = cfg;
    this.logger = loggerFactory.getLogger(LuceneIndexManager.class);
    this.perCacheIdxCt = cfg.indexesPerCache();

    logger.info("Lucene version: " + Constants.LUCENE_MAIN_VERSION);

    queryThreadPool = createQueryThreadPool(cfg.maxConcurrentQueries(), perCacheIdxCt);

    this.indexDir = indexDir;

    boolean useRamdir = cfg.useRamDir();
    boolean useOffHeapdir = cfg.useOffHeap();
    if (useRamdir && useOffHeapdir) { throw new AssertionError(
                                                               "Can't have both Ram Directory and OffHeap Directory enabled !"); }

    if (isPermStore && (useRamdir || useOffHeapdir)) logger
        .warn("Persistent mode is specified - ignoring ram/offheap directory setting.");

    ramdir = !isPermStore && useRamdir;
    offHeapdir = !isPermStore && useOffHeapdir;
    if (ramdir) {
      logger.warn("Using on-heap ram directory for search indices. Heap usage is unbounded");
    }

    if (offHeapdir) {
      logger.info("Using off-heap directory for search indices ");
      this.offHeapFileSystem = new OffheapFileSystem(pageSource, cfg.getOffHeapFileBlockSize(),
                                                     cfg.getOffHeapFileMaxPageSize(), cfg.getOffHeapFileSegmentCount());
    } else {
      this.offHeapFileSystem = null;
    }
  }

  private static ExecutorService createQueryThreadPool(int maxConcurrentQueries, int idxPerCache) {
    return Executors.newFixedThreadPool(maxConcurrentQueries * idxPerCache,
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

      for (File dir : indexDir.listFiles(dirsOnly)) {
        try {
          boolean isCleanGroup = true;

          // Can't load indexes without a schema!
          File schemaFile = new File(dir, TERRACOTTA_SCHEMA_FILE);
          File[] subDirs = dir.listFiles(dirsOnly);
          if (!schemaFile.canRead() || subDirs.length != perCacheIdxCt) {
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
          // We MUST use the original cache name for index group creation, for consistency with on-demand group
          // creation during call chain from upsert()
          if (isCleanGroup) getOrCreateGroup(loadName(dir), null);
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

  public void initIndexSchema(String indexName, Map<String, Class<?>> schema) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group == null) {
      synchronized (idxGroups) {
        group = getGroup(indexName);
        // double check for not creating indices again
        if (group == null) {
          group = new IndexGroup(indexName, false);
          IndexGroup prev = this.idxGroups.put(indexName, group);
          if (prev != null) { throw new AssertionError("replaced group for " + indexName); }
        }
      }
      group.storeName();
      Map<String, AttributeProperties> idxSchema;
      synchronized (group) {
        idxSchema = convertSchema(schema);
        group.storeSchema(idxSchema);
        group.schema.putAll(idxSchema);
      }
    } else logger.warn("Attempting to pass in new schema to existing index: " + indexName);

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
    if (!indexDir.isDirectory()) return new String[0];
    String[] res = new String[indexDir.listFiles(dirsOnly).length];
    int i = 0;
    try {
      for (File idx : indexDir.listFiles(dirsOnly)) {
        res[i++] = loadName(idx);
      }
      return res;
    } catch (IndexException e) {
      throw new RuntimeException(e);
    }
  }

  private void destroyGroup(String indexName) throws IndexException {
    final IndexGroup group;

    synchronized (idxGroups) {
      group = idxGroups.remove(indexName);
    }

    if (group == null) { return; }

    logger.info("Destroying existing search index for " + indexName);

    group.close(true);
    try {
      Util.deleteDirectory(group.getPath());
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  private IndexGroup getOrCreateGroup(String name, List<NVPair> attrs) throws IndexException {
    IndexGroup group = getGroup(name);
    if (group == null) {
      synchronized (idxGroups) {
        group = getGroup(name);
        // double check for not creating indices again
        if (group == null) {
          group = new IndexGroup(name, attrs == null);
          IndexGroup prev = this.idxGroups.put(name, group);
          if (prev != null) { throw new AssertionError("replaced group for " + name); }
        }
      }
      group.storeName();
      Map<String, AttributeProperties> idxSchema;
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

  SearchResult searchIndex(String name, Search s) throws IndexException {
    return searchIndex(name, s.getRequesterId(), s.getQueryId(), s.getQueryStack(), s.isIncludeKeys(),
                       s.isIncludeValues(), s.getAttributes(),
                       s.getGroupByAttrs(), s.getSortAttributes(), s.getAggregatorList(), s.getMaxResults(),
                       s.getBatchSize());
  }

  public SearchResult searchIndex(String name, long requesterId, long queryId, final List queryStack,
                                  final boolean includeKeys,
                                  final boolean includeValues, final Set<String> attributeSet,
                                  Set<String> groupByAttributes, final List<NVPair> sortAttributes,
                                  final List<NVPair> aggregators, final int maxResults, final int prefetchSize)
                                      throws IndexException {
    IndexGroup indexes = getGroup(name);
    if (indexes == null) {
      // Return empty set, since index might not exist on this stripe
      return SearchResult.NULL_RESULT;
    }

    return indexes.searchIndex(requesterId, queryId, queryStack, includeKeys, includeValues, attributeSet,
                               groupByAttributes, sortAttributes,
                               aggregators, maxResults, prefetchSize);

  }

  public synchronized void shutdown() throws IndexException {
    if (shutdown) return;
    searchMonitor.shutdown();
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
                                 ProcessingContext context, boolean fromEviction) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.removeIfValueEqual(toRemove, segmentId, context);
    } else {
      // Suppress logging if this is an eviction (DEV-7487)
      if (!fromEviction) logger.info("RemoveIfValueEqual ignored: no such index group [" + indexName + "] exists");
      context.processed();
    }
  }

  public void update(String indexName, String key, ValueID value, List<NVPair> attributes,
                     List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context) throws IndexException {
    // Get or create here b/c draining the journal will do a blind update due to a possible race with index sync
    IndexGroup group = getOrCreateGroup(indexName, attributes);
    group.update(key, value, attributes, storeOnlyAttributes, segmentId, context);
  }

  public void insert(String indexName, String key, ValueID value, List<NVPair> attributes,
                     List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context) throws IndexException {
    IndexGroup group = getOrCreateGroup(indexName, attributes);
    group.insert(key, value, attributes, storeOnlyAttributes, segmentId, context);
  }

  public void putIfAbsent(String indexName, String key, ValueID value, List<NVPair> attributes,
                          List<NVPair> storeOnlyAttributes, long segmentId, ProcessingContext context)
      throws IndexException {
    IndexGroup group = getOrCreateGroup(indexName, attributes);
    group.putIfAbsent(key, value, attributes, storeOnlyAttributes, segmentId, context);
  }

  public void destroy(String indexName, ProcessingContext context) throws IndexException {
    destroyGroup(indexName);
    context.processed();
  }

  public void clear(String indexName, long segmentId, ProcessingContext context) throws IndexException {
    IndexGroup group = getGroup(indexName);
    if (group != null) {
      group.clear(segmentId, context);
    } else {
      context.processed();
    }
  }

  private static Map<String, AttributeProperties> extractSchema(List<NVPair> attributes) throws IndexException {
    Map<String, AttributeProperties> schema = new HashMap<String, AttributeProperties>(attributes.size());

    for (NVPair attr : attributes) {
      AttributeProperties prev = schema.put(attr.getName(), new AttributeProperties(attr, true));
      if (prev != null && !getAttributeTypeString(attr).equals(prev.getType())) {
        //
        throw new IndexException("Differing types for repeated attribute: " + attr.getName());
      }
    }

    return schema;
  }
  
  private static Map<String, AttributeProperties> convertSchema(Map<String, Class<?>> configSchema)
      throws IndexException {
    Map<String, AttributeProperties> schema = new HashMap<String, LuceneIndexManager.AttributeProperties>(
        configSchema.size());
    for (Map.Entry<String, Class<?>> entry : configSchema.entrySet()) {
      String attrName = entry.getKey();
      checkAttributeName(attrName);

      Class<?> attrType = entry.getValue();
      ValueType vType = ValueType.valueOf(attrType);
      if (vType == null) throw new IndexException(String.format("Unsupported type %s specified for attribute %s",
                                                                attrType.getName(), attrName));
      boolean isEnum = vType == ValueType.ENUM;
      schema.put(attrName, new AttributeProperties(isEnum ? attrType.getName() : vType.name(), true, isEnum));
    }
    return schema;
  }

  public SyncSnapshot snapshot(final String id) throws IndexException {
    final Map<String, List<IndexFile>> filesToSync = getFilesToSync(id);

    return new SyncSnapshot() {
      @Override
      public void release() {
        for (String name : filesToSync.keySet()) {
          releaseSyncSnapshot(id, name);
        }
      }

      @Override
      public Map<String, List<IndexFile>> getFilesToSync() {
        return filesToSync;
      }
    };
  }

  public void snapshotIndex(String id, QueryID queryId, ProcessingContext ctxt) throws IndexException {
    IndexGroup group = getGroup(id);
    if (group != null) {
      group.takeSnapshot(queryId, ctxt);
    } else {
      ctxt.processed();
    }
    
  }
  
  public InputStream getIndexFile(String cacheName, String indexId, String fileName) throws IOException {
    IndexGroup group = getGroup(cacheName);
    if (group == null) { throw new AssertionError("missing index group for " + cacheName); }

    return group.getIndexFile(indexId, fileName);
  }

  private void releaseSyncSnapshot(String syncId, String name) {
    IndexGroup group = getGroup(name);
    if (group != null) {
      group.releaseSyncSnapshot(syncId);
    } else {
      logger.error("No such index group [" + name + "] exists to release");
    }
  }

  private Map<String, List<IndexFile>> getFilesToSync(String syncId) throws IndexException {
    Map<String, List<IndexFile>> filesSyncMap = new HashMap();
    for (Map.Entry<String, IndexGroup> entry : idxGroups.entrySet()) {

      String cacheName = entry.getKey();
      List<IndexFile> idxFiles = entry.getValue().getSyncFiles(syncId);
      if (idxFiles != null) filesSyncMap.put(cacheName, idxFiles);
    }
    return filesSyncMap;
  }

  public void applyIndexSync(String cacheName, String indexId, String fileName, byte[] fileData, boolean isTCFile,
                             boolean isLast) throws IOException {
    if (!ramdir && !offHeapdir || isTCFile) {
      File indexFile = resolveIndexFilePath(indexDir, cacheName, indexId, fileName);
      indexFile.createNewFile();
      FileOutputStream fos = new FileOutputStream(indexFile, true);
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
        output = dir.createOutput(fileName, IOContext.DEFAULT);
        tempRamOutput.put(dir, output);
      }

      output.writeBytes(fileData, fileData.length);

      if (isLast) {
        output.close();
        tempRamOutput.remove(dir);
      }
    }
  }

  static File resolveIndexFilePath(File indexDir, String cacheName, String indexId, String fileName) throws IOException {
    File cacheIndexDir = new File(indexDir, Util.sanitizeCacheName(cacheName));
    if (indexId != null) cacheIndexDir = new File(cacheIndexDir, indexId);
    Util.ensureDirectory(cacheIndexDir);
    return new File(cacheIndexDir, fileName);
  }

  public void backup(File destDir, SyncSnapshot snapshot) throws IOException {
    if (ramdir || offHeapdir) {
      throw new UnsupportedOperationException("Backups not supported for non-persistent index.");
    }
    try {
      for (Map.Entry<String, List<IndexFile>> e : snapshot.getFilesToSync().entrySet()) {
        String cacheName = e.getKey();
        for (IndexFile indexFile : e.getValue()) {
          File destIndexFile = resolveIndexFilePath(destDir, cacheName, indexFile.getIndexId(), indexFile.getDestFilename());
          File sourceIndexFile = resolveIndexFilePath(indexDir, cacheName, indexFile.getIndexId(), indexFile.getLuceneFilename());
          IOUtils.copy(sourceIndexFile, destIndexFile);
        }
      }
    } finally {
      snapshot.release();
    }
  }

  SearchResult loadResultSet(String cacheName, Search s, int offset, int batchSize) throws IndexException {
    return loadResultSet(cacheName, s.getRequesterId(), s.getQueryId(), s.getQueryStack(), s.isIncludeKeys(), s.isIncludeValues(),
                         s.getAttributes(), s.getSortAttributes(), s.getAggregatorList(), s.getMaxResults(), offset, batchSize);
  }
  
  SearchResult loadResultSet(long clientId, long queryId, String cacheName, int offset, int fetchSize) throws IndexException {
    IndexGroup group = getGroup(cacheName);
    if (group != null) {
      return group.loadResults(clientId, queryId, offset, fetchSize);
    } else throw new IndexException(String.format("Index for cache %s does not exist.", cacheName));
  }
  
  public SearchResult loadResultSet(String cacheName, long clientId, long queryId, final List queryStack,
                                    final boolean includeKeys,
                                    final boolean includeValues, final Set<String> attributeSet,
                                    final List<NVPair> sortAttributes,
                                    final List<NVPair> aggregators, final int maxResults,
                                    final int offset, final int fetchSize)
      throws IndexException {
    IndexGroup group = getGroup(cacheName);
    if (group != null) {
      return group.loadResults(clientId, queryId, queryStack,
                               includeKeys,
                               includeValues, attributeSet,
                               sortAttributes,
                               aggregators, maxResults, offset, fetchSize);
    } else throw new IndexException(String.format("Index for cache %s does not exist.", cacheName));
    
  }
  
  public void releaseResultSet(String cacheName, QueryID queryId, ProcessingContext ctxt) throws IndexException {
    IndexGroup group = getGroup(cacheName);
    if (group != null) {
      group.closeResultSet(queryId, ctxt);
    } else ctxt.processed();
  }

  public void releaseAllResultsFor(long clientId) throws IndexException {
    for (IndexGroup group : idxGroups.values()) {
      group.closeAllResults(clientId);
    }
  }
  
  public void pruneSearchResults(Set<Long> activeClients) throws IndexException {
    for (IndexGroup group : idxGroups.values()) {
      group.pruneSearchResults(activeClients);
    }
  }

  private static String getDirName(String cacheName, String idxId) {
    return cacheName + File.separator + idxId;
  }

  private static void checkAttributeName(String attrName) throws IndexException {
    if (attrName.equals(LuceneIndex.KEY_FIELD_NAME) || attrName.equals(LuceneIndex.VALUE_FIELD_NAME)
        || attrName.equals(LuceneIndex.SEGMENT_ID_FIELD_NAME)) {
      // XXX: this assertion needs coverage at a higher level too (eg. ehcache level maybe?)
      throw new IndexException("Illegal attribute name present: " + attrName);
    }
  }

  static String getAttributeTypeString(NVPair nvpair) {
    return nvpair instanceof NVPairEnum ? ((NVPairEnum) nvpair).getClassName() : nvpair.getType().name();
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

  final class IndexGroup implements IndexOwner {
    private final ConcurrentMap<Integer, LuceneIndex>        indices = new ConcurrentHashMap<Integer, LuceneIndex>(
                                                                                                                   perCacheIdxCt);
    private final String                                     groupName;
    private final ConcurrentMap<String, AttributeProperties> schema  = new ConcurrentHashMap<String, AttributeProperties>();
    private final Map<String, File>                          schemaSnapshots = new HashMap<String, File>();
    private final Timer                                      searcherRefreshTimer;
    private final SearchResultsManager                       resultsMgr;
    private final File                                       groupFile;

    private IndexGroup(String name, boolean load) throws IndexException {
      groupName = name;

      // always set the type for our internal fields
      schema.put(LuceneIndex.KEY_FIELD_NAME, new AttributeProperties(ValueType.STRING.name(), true, false));
      schema.put(LuceneIndex.VALUE_FIELD_NAME, new AttributeProperties(ValueType.LONG.name(), true, false));
      schema.put(LuceneIndex.SEGMENT_ID_FIELD_NAME, new AttributeProperties(ValueType.LONG.name(), true, false));

      searcherRefreshTimer = new Timer(groupName + "-searcherRefreshTask", true);
      try {
        groupFile = Util.getOrCreateCacheGroupDir(indexDir, groupName, load);
        createIndices(load);
        resultsMgr = new SearchResultsManager(getPath(), this, cfg, queryThreadPool, loggerFactory, searchMonitor);
      } catch (IOException x) {
        throw new IndexException(x);
      }
    }

    @Override
    public String getName() {
      return groupName;
    }

    private void close(boolean waitForMerges) throws IndexException {
      searcherRefreshTimer.cancel();
      resultsMgr.shutdown();
      for (LuceneIndex index : indices.values()) {
        index.close(waitForMerges);
      }
    }

    private void close() throws IndexException {
      close(false);
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

    private synchronized void releaseSyncSnapshot(String syncId) {
      File schemaSnapshot = schemaSnapshots.remove(syncId);
      if (schemaSnapshot != null && schemaSnapshot.exists()) {
        boolean deleted = schemaSnapshot.delete();
        if (!deleted) {
          logger.warn("failed to delete temp schema snapshot: " + schemaSnapshot.getAbsolutePath());
        }
      }

      for (LuceneIndex index : indices.values())
        index.releaseSyncSnapshot(syncId);

    }

    private synchronized List<IndexFile> getSyncFiles(String syncId) throws IndexException {
      List<IndexFile> files = new ArrayList<IndexFile>();
      // Add TC files first so they will exist before index opened on a passive syncing

      File schemaFile = new File(getPath(), TERRACOTTA_SCHEMA_FILE);
      File snapshot;
      if (schemaFile.exists()) {
        try {
          snapshot = File.createTempFile("tmp", TERRACOTTA_SCHEMA_FILE, getPath());
          schemaSnapshots.put(syncId, snapshot);
          Util.copyFile(schemaFile, snapshot);
        } catch (IOException e) {
          throw new IndexException(e);
        }
        files.add(new IndexFileImpl(TERRACOTTA_SCHEMA_FILE, snapshot.getName(), null, true, snapshot
            .length()));
      } else {
        logger.info("Schema file doesn't exist: " + schemaFile);
      }

      // include the cache name file (it never changes so no need for temp copy)
      files.add(new IndexFileImpl(TERRACOTTA_CACHE_NAME_FILE, TERRACOTTA_CACHE_NAME_FILE, null, true,
                                  new File(getPath(), TERRACOTTA_CACHE_NAME_FILE).length()));
      // include the hashed cache name file
      files.add(new IndexFileImpl(TERRACOTTA_HASH_FILE, TERRACOTTA_HASH_FILE, null, true,
          new File(getPath(), TERRACOTTA_HASH_FILE).length()));

      for (LuceneIndex idx : indices.values()) {
        List<IndexFile> idxFiles = idx.getSyncSnapshot(syncId);
        if (idxFiles != null) files.addAll(idxFiles);
      }
      return files;
    }
    
    private synchronized void takeSnapshot(QueryID queryId, ProcessingContext context) throws IndexException {
      try {
        for (LuceneIndex idx : indices.values()) {
          idx.storeSnapshot(queryId);
        }
      } finally {
        context.processed();
      }
    }

    // public for test purposes.
    public void closeResultSet(QueryID id, ProcessingContext ctxt) throws IndexException {
      try {
        resultsMgr.resultsProcessed(id);
      } finally {
        for (LuceneIndex idx : indices.values()) {
          idx.releaseQueryCommit(id);
        }
        ctxt.processed();
      }

    }

    private void closeAllResults(long requesterId) throws IndexException {
      try {
        resultsMgr.clearResultsFor(requesterId);
      } finally {
        for (LuceneIndex idx : indices.values()) {
          idx.releaseQueryCommit(requesterId);
        }
      }
    }

    private void pruneSearchResults(Set<Long> activeClients) throws IndexException {
      try {
        resultsMgr.pruneResults(activeClients);
      } finally {
        for (LuceneIndex idx : indices.values()) {
          idx.pruneCommits(activeClients);
        }
      }
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

    private void putIfAbsent(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                             long segmentId, ProcessingContext context) throws IndexException {
      LuceneIndex index = getIndex(segmentId);
      if (index == null) throw new IndexException("Unable to run putIfAbsent: segment " + segmentId
                                                  + " has no index in group " + groupName);
      index.putIfAbsent(key, value, attributes, storeOnlyAttributes, segmentId, context);
    }

    private SearchResult loadResults(long clientId, long queryId, int offset, int fetchSize) throws IndexException {
      int batchLimit = Math.min(cfg.getMaxResultBatchSize(), fetchSize);
      SearchResult res = resultsMgr.loadResults(clientId, queryId, offset, batchLimit);
      if (res == null) throw new IndexException(String.format("Results for query %s unavailable", queryId));
      return res;
    }
    
    private SearchResult loadResults(long clientId, long queryId, final List queryStack,
                                     final boolean includeKeys,
                                     final boolean includeValues, final Set<String> attributeSet,
                                     final List<NVPair> sortAttributes,
                                     final List<NVPair> aggregators, final int maxResults,
                                     final int offset, final int fetchSize)
      throws IndexException {
      int batchLimit = Math.min(cfg.getMaxResultBatchSize(), fetchSize);
      SearchResult res = resultsMgr.loadResults(clientId, queryId, offset, batchLimit);
      // Re-run search query if unable to load from previous state
      if (res == null) {
        logger.info(String.format("Unable to load previously saved search result set for requester %d and query id %d; re-executing original query", clientId, queryId));
        searchIndex(clientId, queryId, queryStack, includeKeys, includeValues, attributeSet, Collections.<String>emptySet(), sortAttributes, aggregators, maxResults, fetchSize);
        res = resultsMgr.loadResults(clientId, queryId, offset, batchLimit);
        if (res == null) throw new IndexException(String.format("Results for query %s unavailable", queryId));
      }
      return res;
    }
    
    private SearchResult searchIndex(final long requesterId, final long queryId, final List queryStack,
                                     final boolean includeKeys, final boolean includeValues,
                                     final Set<String> attributeSet, final Set<String> groupByAttributes,
                                     final List<NVPair> sortAttributes, final List<NVPair> aggPairs,
                                     final int maxResults, final int prefetchSize) throws IndexException {

      IndexReader[] readers = new IndexReader[indices.size()];
      int i = 0;
      int batchLimit = Math.min(cfg.getMaxResultBatchSize(), prefetchSize);
      boolean isPagedSearch = batchLimit != Search.BATCH_SIZE_UNLIMITED;
      
      try {
        QueryID qId = new QueryID(requesterId, queryId);
        for (LuceneIndex idx : indices.values()) {
          readers[i++] = isPagedSearch ? idx.getReader(qId) : idx.getReader();
        }
        return resultsMgr.executeQuery(qId, queryStack, readers, includeKeys, includeValues,
                                       attributeSet, groupByAttributes, sortAttributes, aggPairs, maxResults,
                                       batchLimit);
      } catch (IOException iox) {
        throw new IndexException(iox);
      } finally {
        for (IndexReader r : readers) {
          try {
            if (r != null) r.decRef(); // do not leak reader refs
          } catch (IOException x) {
            // ignored here
          }
        }
      }

    }

    private File getPath() {
      return groupFile;
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
            luceneIndex = new LuceneIndex(createDirectoryFor(path, groupName), idxStr, path, this,
                                          cfg, loggerFactory);
          } else {
            luceneIndex = new LuceneIndex(directoryFor(groupName, idxStr, path), idxStr, path, this,
                                          cfg, loggerFactory, true); // FIXME
            logger.info(String.format("Opening existing search index [%s/%d]", groupName, idxSegment));
          }
          indices.put(idxSegment, luceneIndex);
        }

      }
    }

    @Override
    public void checkSchema(List<NVPair> attributes, boolean indexed) throws IndexException {
      for (NVPair nvpair : attributes) {
        String attrName = nvpair.getName();
        checkAttributeName(attrName);

        AttributeProperties attrProps = schema.get(attrName);
        if (attrProps == null) {
          // a new attribute name -- update schema
          synchronized (this) {
            // check again since races allowed up until this point
            attrProps = schema.get(attrName);

            if (attrProps == null) {
              attrProps = new AttributeProperties(nvpair, indexed);
              Map<String, AttributeProperties> clone = new HashMap<String, AttributeProperties>(schema);
              AttributeProperties prev = clone.put(attrName, attrProps);
              if (prev != null) { throw new AssertionError("replaced mapping for " + attrName); }

              // attempt disk update
              logger.info("Updating stored schema");
              storeSchema(clone);

              // disk update okay -- now change memory
              prev = schema.put(attrName, attrProps);
              if (prev != null) { throw new AssertionError("replaced mapping for " + attrName); }
            }
          }
        }

        String type = attrProps.getType();
        String typeString = getAttributeTypeString(nvpair);

        if (!type.equals(typeString)) {
          //
          throw new IndexException("Attribute type (" + typeString + ") does not match schema type (" + type + ")");
        }
      }

    }

    @Override
    public Map<String, AttributeProperties> getSchema() {
      return Collections.unmodifiableMap(schema);
    }

    @Override
    public Timer getReaderRefreshTimer() {
      return searcherRefreshTimer;
    }

    private void storeSchema(Map<String, AttributeProperties> schemaToStore) throws IndexException {
      File tmp;
      File path = getPath();
      try {
        tmp = File.createTempFile("tmp", TERRACOTTA_SCHEMA_FILE, path);
      } catch (IOException e) {
        throw new IndexException(e);
      }

      Properties props = new Properties();

      for (Map.Entry<String, AttributeProperties> entry : schemaToStore.entrySet()) {
        props.setProperty(entry.getKey(), entry.getValue().getType() + "," + entry.getValue().isIndexed() + ","
                                          + entry.getValue().isEnum());
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

    private Map<String, AttributeProperties> loadSchema() throws IndexException {
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

      Map<String, AttributeProperties> res = new HashMap<String, AttributeProperties>();

      for (Enumeration<String> i = (Enumeration<String>) data.propertyNames(); i.hasMoreElements();) {
        String key = i.nextElement();
        String value = data.getProperty(key).trim();

        String[] split = value.split(",");
        if (split.length != 3) { throw new IndexException("Unexpected format: " + value); }

        String typeName = split[0];
        String isEnum = split[2];

        if (!"false".equals(isEnum) && !"true".equals(isEnum)) {
          //
          throw new IndexException("Unexpected format for indexed: " + isEnum);
        }
        if (!Boolean.valueOf(isEnum)) {
          try {
            Enum.valueOf(ValueType.class, typeName);
          } catch (IllegalArgumentException iae) {
            throw new IndexException("No such type (" + typeName + ") for key " + key);
          }
        }

        String indexed = split[1];
        if (!"false".equals(indexed) && !"true".equals(indexed)) {
          //
          throw new IndexException("Unexpected format for indexed: " + indexed);
        }

        res.put(key, new AttributeProperties(typeName, Boolean.valueOf(indexed), Boolean.valueOf(isEnum)));
      }
      return res;
    }

    private void storeName() throws IndexException {
      try {
        Util.storeContent(new File(getPath(), TERRACOTTA_CACHE_NAME_FILE), groupName);
      } catch (IOException e) {
        throw new IndexException(e);
      }
    }

  }

  /**
   * Load "unsanitized" cache name
   */
  private static String loadName(File path) throws IndexException {
    try {
      return Util.loadContent(new File(path, TERRACOTTA_CACHE_NAME_FILE));
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  static class AttributeProperties {
    private final String  type;
    private final boolean   indexed;
    private final boolean isEnum;

    AttributeProperties(String type, boolean indexed, boolean isEnum) {
      this.type = type;
      this.indexed = indexed;
      this.isEnum = isEnum;
    }

    private AttributeProperties(NVPair nvpair, boolean isIndexed) {
      this.isEnum = nvpair instanceof NVPairEnum;
      this.type = getAttributeTypeString(nvpair);
      this.indexed = isIndexed;
    }

    String getType() {
      return type;
    }

    boolean isIndexed() {
      return indexed;
    }

    boolean isEnum() {
      return isEnum;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + type + ",indexed=" + indexed + ")";
    }
  }

}

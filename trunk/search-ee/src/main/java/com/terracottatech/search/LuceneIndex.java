/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.AbstractNVPair.BooleanNVPair;
import com.terracottatech.search.AbstractNVPair.ByteArrayNVPair;
import com.terracottatech.search.AbstractNVPair.ByteNVPair;
import com.terracottatech.search.AbstractNVPair.CharNVPair;
import com.terracottatech.search.AbstractNVPair.DateNVPair;
import com.terracottatech.search.AbstractNVPair.DoubleNVPair;
import com.terracottatech.search.AbstractNVPair.EnumNVPair;
import com.terracottatech.search.AbstractNVPair.FloatNVPair;
import com.terracottatech.search.AbstractNVPair.IntNVPair;
import com.terracottatech.search.AbstractNVPair.LongNVPair;
import com.terracottatech.search.AbstractNVPair.ShortNVPair;
import com.terracottatech.search.AbstractNVPair.SqlDateNVPair;
import com.terracottatech.search.AbstractNVPair.StringNVPair;
import com.terracottatech.search.AbstractNVPair.ValueIdNVPair;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.Version;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class LuceneIndex {

  private static final ExecutorService     s_commitThreadPool    =
      Executors.newSingleThreadExecutor(new NamedThreadFactory("index-commit-pool") {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = super.newThread(r);
          t.setDaemon(true);
          return t;
        }

      });
  private static final long                READER_REFRESH_RATE = 60000;
  private static final String QUERY_ID_FIELD = "queryId";
  private static final String CLIENT_ID_FIELD = "clientId";
  private final ExplicitBlockingDeletePolicy hardSnapshotter;

  private static final class NonIndexedFieldType extends FieldType {
    private static final NonIndexedFieldType INT    = new NonIndexedFieldType(NumericType.INT);
    private static final NonIndexedFieldType LONG   = new NonIndexedFieldType(NumericType.LONG);
    private static final NonIndexedFieldType DOUBLE = new NonIndexedFieldType(NumericType.DOUBLE);
    private static final NonIndexedFieldType FLOAT  = new NonIndexedFieldType(NumericType.FLOAT);

    private NonIndexedFieldType(NumericType numType) {
      super(IntField.TYPE_STORED);
      setIndexed(false);
      setNumericType(numType);
      freeze();
    }
  }

  static final String                      TERRACOTTA_INIT_FILE  = "__terracotta_init.txt";

  static final String                      SEGMENT_ID_FIELD_NAME = "__TC_SEGMENT_ID";
  public static final String               KEY_FIELD_NAME        = "__TC_KEY_FIELD";
  static final String                      VALUE_FIELD_NAME      = "__TC_VALUE_FIELD";

  private final Directory                  luceneDirectory;
  private final PersistentSnapshotDeletionPolicy     snapshotter;
  private final IndexWriter                writer;
  private final ReaderManager              readerMgr;
  private final File                       path;
  private final String                     name;
  private Future<?>                        committer;
  private final AtomicBoolean              shutdown              = new AtomicBoolean();
  private final Logger                     logger;
  private final boolean                    useCommitThread;

  private List<ProcessingContext>          pending               = newPendingList();
  private final IndexOwner                 idxGroup;

  private final AtomicReference<Thread>    accessor              = new AtomicReference<Thread>();

  private final boolean                    doAccessCheck;

  private final ReentrantLock              commitLock            = new ReentrantLock();
  
  /**
   * Map of snapshotted commit points by id; non-durable
   */
  private final Map<String, IndexCommit>   snapshotCommits = Collections.synchronizedMap(new HashMap<String, IndexCommit>());

  /**
   * Map of snapshotted commit points by query id, these are recorded in commit user data and reloaded on startup
   */
  private final ConcurrentMap<QueryID, IndexCommit>   queryCommits = new ConcurrentHashMap<QueryID, IndexCommit>();

  private final Map<String, File> snapshotCopies = new HashMap<String, File>();
  
  LuceneIndex(Directory directory, String name, File path, IndexOwner parent,
              Configuration cfg, LoggerFactory loggerFactory) throws IndexException {
    this(directory, name, path, parent, cfg, loggerFactory, false);
  }
  
  LuceneIndex(Directory directory, String name, File path, IndexOwner parent,
              Configuration cfg, LoggerFactory loggerFactory, boolean existing) throws IndexException {
    this.doAccessCheck = cfg.doAccessChecks();

    this.logger = loggerFactory.getLogger(getClass().getName() + "-" + path.getName());
    this.path = path;
    this.name = name;
    this.luceneDirectory = directory;
    this.useCommitThread = cfg.useCommitThread();
    this.idxGroup = parent;

    try {
      Util.ensureDirectory(path);
      this.hardSnapshotter = new ExplicitBlockingDeletePolicy(new KeepOnlyLastCommitDeletionPolicy());
      this.snapshotter = new PersistentSnapshotDeletionPolicy(hardSnapshotter, directory);
      IndexWriterConfig idxWriterCfg = new IndexWriterConfig(Version.LUCENE_46, null);
      idxWriterCfg.setIndexDeletionPolicy(snapshotter);
      idxWriterCfg.setRAMBufferSizeMB(cfg.getMaxRamBufferSize());
      idxWriterCfg.setMaxBufferedDocs(cfg.getMaxBufferedDocs());
      if (logger.isDebugEnabled()) {
        idxWriterCfg.setInfoStream(new TCInfoStream(this.logger));
      }

      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
      cms.setMaxMergesAndThreads(cfg.getMaxMergeCount(), cfg.getMaxMergeThreadCount());
      idxWriterCfg.setMergeScheduler(cms);

      if (cfg.disableStoredFieldCompression()) {
        idxWriterCfg.setCodec(new DisableCompressionCodec());
      } else {
        Codec cd = new OptimizedCompressionCodec();
        idxWriterCfg.setCodec(cd);
        logger.info("Using stored fields format: " + cd.storedFieldsFormat());
      }

      writer = new IndexWriter(luceneDirectory, idxWriterCfg);
      
      IndexCommit currCommit = null;
      // Do all this before the first commit to the index
      for (IndexCommit ic: snapshotter.getSnapshots()) {
        Map<String, String> commitData = ic.getUserData();
        if (!commitData.isEmpty()) {
          QueryID id = new QueryID(Long.valueOf(commitData.get(CLIENT_ID_FIELD)), Long.valueOf(commitData.get(QUERY_ID_FIELD)));
          queryCommits.put(id, ic);
        } else {
          if (currCommit != null)
            throw new IllegalStateException("Reopened index contains multiple commit points from prior index states: " +
                ic.getSegmentsFileName());
          currCommit = ic;
        }
      }
      
      // release current commit point for reopened indexes
      if (currCommit != null) {
        if (!existing) throw new IllegalStateException("Found prior commit point on index open in create mode: " +
            currCommit.getSegmentsFileName());
        
        snapshotter.release(currCommit);
      }
      
      writer.commit(); // make sure all index metadata is written out
      readerMgr = new ReaderManager(writer, true);
      parent.getReaderRefreshTimer().schedule(new TimerTask() {

        @Override
        public void run() {
          try {
            readerMgr.maybeRefresh();
          } catch (IOException e) {
            logger.error(e);
          }
        }
      }, READER_REFRESH_RATE, READER_REFRESH_RATE);
      
      logger.info("Directory type: " + luceneDirectory.getClass().getName() + " with max buffer size "
                  + idxWriterCfg.getRAMBufferSizeMB() + ", disableDocCompression="
                  + cfg.disableStoredFieldCompression());

    } catch (IOException e) {
      throw new IndexException(e);
    }

    // this should be the last action of this constructor. This marker file indicates that the index
    // directory was successfully initialized (DEV-5573)
    try {
      createInitFile();
    } catch (IOException ioe) {
      try {
        writer.close();
      } catch (IOException ioe2) {
        logger.error(ioe2);
      }

      throw new IndexException(ioe);
    }
  }

  String getName() {
    return name;
  }

  InputStream getIndexFile(String fileName) throws IOException {
    File file = new File(path, fileName);
    if (file.isFile()) { return new BufferedInputStream(new FileInputStream(file)); }

    // if (!(luceneDirectory instanceof RAMDirectory)) {
    //
    // return null;
    // throw new AssertionError("Not a RAMDirectory : " + luceneDirectory.getClass().getName());
    // }

    return new IndexInputAdapter(luceneDirectory.openInput(fileName, IOContext.READ));
  }

  void optimize() throws CorruptIndexException, IOException {
    writer.forceMerge(1); // Don't do this!
  }

  protected void createInitFile() throws IOException {
    new File(path, TERRACOTTA_INIT_FILE).createNewFile();
  }

  static boolean hasInitFile(File path) {
    return new File(path, TERRACOTTA_INIT_FILE).isFile();
  }

  private List<ProcessingContext> newPendingList() {
    return new ArrayList<ProcessingContext>(256);
  }

  List<IndexFile> getSyncSnapshot(String syncId) throws IndexException {
    try {
      writer.commit();
      hardSnapshotter.pinDeletions();
      if (logger.isDebugEnabled()) {
        logger.debug("Taking snapshot for index: " + name);
      }

      List<IndexFile> files = new ArrayList();

      // include the init file so caches on the other end won't be immediately deleted upon open :-)
      files.add(new IndexFileImpl(TERRACOTTA_INIT_FILE, TERRACOTTA_INIT_FILE, name, true,
                                  new File(path, TERRACOTTA_INIT_FILE).length()));

      // Add lucene files last (ie. after TC special files)
      IndexCommit ic = this.snapshotter.snapshot();
      snapshotCommits.put(syncId, ic);
      
      List<IndexCommit> toSnapshot = new ArrayList<IndexCommit>();
      toSnapshot.add(ic);
      for (IndexCommit commit: snapshotter.getSnapshots()) {
        // Filter out non-query initiated commits for other sync ids
        if (!commit.getUserData().isEmpty()) toSnapshot.add(commit);
      }
      
      // Not every commit results in completely new set of segments; weed out duplicates (those segments potentially untouched by some commits)
      Set<String> idxFiles = new HashSet<String>(luceneDirectory.listAll().length);
      for (IndexCommit commit: toSnapshot) {
        Collection<String> fileNames = commit.getFileNames();
        idxFiles.addAll(fileNames);
      }
      
      for (String fileName : idxFiles) {
          if (!fileName.endsWith(".lock")) {
            files.add(new IndexFileImpl(fileName, fileName, name, false, luceneDirectory.fileLength(fileName)));
          }
      }
          
      // create temp copy of policy save file and add it to the snapshot
      File snapshotIdx = new File(path, snapshotter.getLastSaveFile());
      File temp;
      if (snapshotIdx.exists()) {
        try {
          temp = File.createTempFile("tmp", snapshotIdx.getName(), path);
          snapshotCopies.put(syncId, temp);
          Util.copyFile(snapshotIdx, temp);
        } catch (IOException e) {
          throw new IndexException(e);
        }
        files.add(new IndexFileImpl(snapshotIdx.getName(), temp.getName(), name, false, temp
            .length()));
      } else {
        logger.info("Snapshot index save file doesn't exist: " + snapshotIdx);
      }

      return files;
    } catch (IOException e) {
      logger.error(e);
      throw new IndexException("Error getting index snapshot", e);
    }
  }

  void releaseSyncSnapshot(String syncId) {
    hardSnapshotter.unpinDeletions();
    File tempCopy = snapshotCopies.remove(syncId);
    if (tempCopy != null && tempCopy.exists()) {
      boolean deleted = tempCopy.delete();
      if (!deleted) {
        logger.warn("failed to delete temp index snapshot save file: " + tempCopy.getAbsolutePath());
      }
    }

    IndexCommit ic = snapshotCommits.remove(syncId);
    releaseSnapshot(ic);
  }
  
  void releaseQueryCommit(QueryID qId) {
    IndexCommit ic = queryCommits.remove(qId);
    releaseSnapshot(ic);
  }
  
  void releaseQueryCommit(long requesterId) {
    for (QueryID id : queryCommits.keySet()) {
      if (requesterId == id.requesterId) {
        releaseQueryCommit(id);
      }
    }
  }
  
  void pruneCommits(Set<Long> activeClients) {
    for (QueryID id : queryCommits.keySet()) {
      if (!activeClients.contains(id.requesterId)) {
        releaseQueryCommit(id);
      }
    }
  }
  
  private void releaseSnapshot(IndexCommit ic) {
    // XXX is this right?
    // if (ic == null) throw new IllegalArgumentException("IndexCommit is null");
    if (ic == null) return;

    if (logger.isDebugEnabled()) {
      logger.info("Releasing snapshot for index");
    }
    try {
      snapshotter.release(ic);
    } catch (IOException e) {
      logger.warn("Unable to release snapshot " + ic.getSegmentsFileName(), e);
    }

  }

  private void addPendingContext(ProcessingContext context) {
    if (useCommitThread) {
      commitLock.lock();
      try {
        pending.add(context);
        if (committer == null) {
          committer = s_commitThreadPool.submit(new CommitTask());
        }
      } finally {
        commitLock.unlock();
      }
    } else {
      context.processed();
    }
  }


  static Object getFieldValue(Document doc, String attrKey, ValueType type) {

    IndexableField field = doc.getField(attrKey);
    if (field == null) return null;
    Number num = field.numericValue();
    String str = field.stringValue();
    BytesRef bytes = field.binaryValue();

    switch (type) {
      case BOOLEAN:
        if (num == null) return null;
        int value = num.intValue();
        if (value == 0) {
          return Boolean.FALSE;
        } else {
          return Boolean.TRUE;
        }
      case BYTE:
        return num == null ? num : num.byteValue();
      case BYTE_ARRAY:
        return bytes == null ? null : bytes.bytes;
      case CHAR:
        return num == null ? null : (char) num.intValue();
      case DATE:
        return num == null ? null : new Date(num.longValue());
      case SQL_DATE:
        return num == null ? null : new java.sql.Date(num.longValue());
      case DOUBLE:
        return num == null ? null : num.doubleValue();
      case FLOAT:
        return num == null ? num : num.floatValue();
      case INT:
        return num == null ? num : num.intValue();
      case LONG:
        return num == null ? num : num.longValue();
      case SHORT:
        return num == null ? num : num.shortValue();
      case STRING:
      case ENUM:
        return str;
      case NULL:
        throw new AssertionError();
      case VALUE_ID:
        return num == null ? null : new ValueID(num.longValue());
    }

    throw new AssertionError(type);
  }

  void remove(String key, ProcessingContext context) throws IndexException {
    checkAccessor();

    try {
      writer.deleteDocuments(new Term(KEY_FIELD_NAME, key));
    } catch (Exception e) {
      context.processed();
      throw new IndexException(e);
    }

    addPendingContext(context);
  }

  void removeIfValueEqual(Map<String, ValueID> toRemove, ProcessingContext context) throws IndexException {
    checkAccessor();
    try {

      for (Entry<String, ValueID> e : toRemove.entrySet()) {
        String key = e.getKey();
        ValueID oidValue = e.getValue();

        BooleanQuery query = new BooleanQuery();
        long vId = oidValue.toLong();
        query.add(new BooleanClause(new TermQuery(new Term(KEY_FIELD_NAME, key)), Occur.MUST));
        query
            .add(new BooleanClause(NumericRangeQuery.newLongRange(VALUE_FIELD_NAME, vId, vId, true, true), Occur.MUST));
        writer.deleteDocuments(query);
      }

      addPendingContext(context);
    } catch (Exception e) {
      context.processed();
      throw new IndexException(e);
    }
  }

  void clear(long segmentOid, ProcessingContext context) throws IndexException {
    try {
      writer.deleteDocuments(NumericRangeQuery.newLongRange(SEGMENT_ID_FIELD_NAME, segmentOid, segmentOid, true, true));
    } catch (Exception e) {
      context.processed();
      throw new IndexException(e);
    }

    addPendingContext(context);
  }

  void replaceIfPresent(String key, ValueID value, ValueID oldValue, List<NVPair> attributes,
                        List<NVPair> storeOnlyAttributes, long segmentOid, ProcessingContext context)
      throws IndexException {
    try {
      ValueID existingValue = valueForKey(key);
      // no-op if no value exists for given key, or this is a 3 arg replace and existing value doesn't match previous
      if (existingValue == null || (oldValue != ValueID.NULL_ID && !existingValue.equals(oldValue))) {
        context.processed();
        return;
      }

      upsertInternal(key, value, attributes, storeOnlyAttributes, segmentOid, false);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  void putIfAbsent(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                   long segmentOid, ProcessingContext context) throws IndexException {
    try {
      ValueID existingValue = valueForKey(key);
      if (existingValue != null) {
        context.processed();
        return;
      }

      upsertInternal(key, value, attributes, storeOnlyAttributes, segmentOid, true);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }

  }
  
  void update(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                     long segmentOid, ProcessingContext context) throws IndexException {
    try {
      upsertInternal(key, value, attributes, storeOnlyAttributes, segmentOid, false);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  void insert(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                     long segmentOid, ProcessingContext context) throws IndexException {
    try {
      upsertInternal(key, value, attributes, storeOnlyAttributes, segmentOid, true);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  // XXX: this assumes single-threaded access to index, i.e. no writes & commits can happen between commit in here and snapshot
  void storeSnapshot(QueryID id) throws IndexException {
    if (queryCommits.containsKey(id)) {
      logger.info(String.format("Snapshot for query id %s already exists: ignoring", id));
      return;
    }
    try {
      Map<String, String> commitData = new HashMap<String, String>(2);
      commitData.put(QUERY_ID_FIELD, String.valueOf(id.queryId));
      commitData.put(CLIENT_ID_FIELD, String.valueOf(id.requesterId));
      writer.setCommitData(commitData);
      writer.commit();
      writer.setCommitData(Collections.<String, String>emptyMap());

      if (logger.isDebugEnabled()) {
        logger.debug("Taking snapshot for index: " + name);
      }

      IndexCommit ic = this.snapshotter.snapshot();
      queryCommits.put(id, ic);
      
    } catch (IOException ie) {
      throw new IndexException(ie);
    }
  }
  
  
  private void upsertInternal(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                              long segmentOid, boolean isInsert) throws IndexException {
    checkAccessor();

    idxGroup.checkSchema(attributes, true);
    idxGroup.checkSchema(storeOnlyAttributes, false);

    Document doc = new Document();

    addKeyField(doc, key);

    addLongField(doc, VALUE_FIELD_NAME, value.toLong(), true);

    // this field is used to implement per-CDSM segment clearing
    addLongField(doc, SEGMENT_ID_FIELD_NAME, segmentOid, true);

    addFields(doc, attributes, true);
    addFields(doc, storeOnlyAttributes, false);

    try {
      if (isInsert) {
        writer.addDocument(doc);
      } else {
        writer.updateDocument(new Term(KEY_FIELD_NAME, key), doc);
      }

    } catch (Exception e) {
      throw new IndexException(e);
    }
  }

  private void checkAccessor() {
    if (doAccessCheck) {
      Thread current = Thread.currentThread();
      if (!accessor.compareAndSet(null, current)) {
        Thread orig = accessor.get();
        if (orig != current) {
          //
          throw new AssertionError("Index is being accessed by a different thread. Original=[" + orig.getName() + "]");
        }
      }
    }
  }

  private static void addKeyField(Document doc, String key) {
    doc.add(new StringField(KEY_FIELD_NAME, key, Store.YES));
  }

  private void addFields(Document doc, List<NVPair> attributes, boolean indexed) throws IndexException {
    for (NVPair nvpair : attributes) {
      String attrName = nvpair.getName();

      ValueType type = nvpair.getType();
      switch (type) {
        case BOOLEAN:
          BooleanNVPair booleanPair = (BooleanNVPair) nvpair;
          addBooleanField(doc, attrName, booleanPair.getValue(), indexed);
          continue;
        case BYTE:
          ByteNVPair bytePair = (ByteNVPair) nvpair;
          addByteField(doc, attrName, bytePair.getValue(), indexed);
          continue;
        case BYTE_ARRAY:
          ByteArrayNVPair byteArrayPair = (ByteArrayNVPair) nvpair;
          addByteArrayField(doc, attrName, byteArrayPair.getValue(), indexed);
          continue;
        case CHAR:
          CharNVPair charPair = (CharNVPair) nvpair;
          addCharField(doc, attrName, charPair.getValue(), indexed);
          continue;
        case DATE:
          DateNVPair datePair = (DateNVPair) nvpair;
          addDateField(doc, attrName, datePair.getValue(), indexed);
          continue;
        case SQL_DATE:
          SqlDateNVPair sqlDatePair = (SqlDateNVPair) nvpair;
          addSqlDateField(doc, attrName, sqlDatePair.getValue(), indexed);
          continue;
        case DOUBLE:
          DoubleNVPair doubleNVPair = (DoubleNVPair) nvpair;
          addDoubleField(doc, attrName, doubleNVPair.getValue(), indexed);
          continue;
        case ENUM:
          EnumNVPair enumPair = (EnumNVPair) nvpair;
          addEnumField(doc, attrName, AbstractNVPair.enumStorageString(enumPair), indexed);
          continue;
        case FLOAT:
          FloatNVPair floatNVPair = (FloatNVPair) nvpair;
          addFloatField(doc, attrName, floatNVPair.getValue(), indexed);
          continue;
        case INT:
          IntNVPair intNVPair = (IntNVPair) nvpair;
          addIntField(doc, attrName, intNVPair.getValue(), indexed);
          continue;
        case LONG:
          LongNVPair longNVPair = (LongNVPair) nvpair;
          addLongField(doc, attrName, longNVPair.getValue(), indexed);
          continue;
        case SHORT:
          ShortNVPair shortNVPair = (ShortNVPair) nvpair;
          addShortField(doc, attrName, shortNVPair.getValue(), indexed);
          continue;
        case STRING:
          StringNVPair stringNVPair = (StringNVPair) nvpair;
          addStringField(doc, attrName, stringNVPair.getValue(), indexed);
          continue;
        case NULL:
          throw new AssertionError();
        case VALUE_ID:
          ValueIdNVPair oidNVPair = (ValueIdNVPair) nvpair;
          addLongField(doc, attrName, oidNVPair.getValue().toLong(), indexed);
          continue;
      }

      throw new AssertionError(type.name());
    }
  }

  private static void addStringField(Document doc, String fieldName, String value, boolean indexed) {
    // normalize to lower case - for indexing ONLY!
    if (indexed) doc.add(createField(fieldName, value.toLowerCase(), StringField.TYPE_NOT_STORED));

    // stored - not indexed
    doc.add(createField(fieldName, value, StoredField.TYPE));
  }

  private static void addShortField(Document doc, String fieldName, short value, boolean indexed) {
    addIntField(doc, fieldName, value, indexed);
  }

  private static void addLongField(Document doc, String fieldName, long value, boolean indexed) {
    doc.add(new LongField(fieldName, value, indexed ? LongField.TYPE_STORED : NonIndexedFieldType.LONG));
  }

  private static void addIntField(Document doc, String attrName, int value, boolean indexed) {
    doc.add(new IntField(attrName, value, indexed ? IntField.TYPE_STORED : NonIndexedFieldType.INT));
  }

  private static void addFloatField(Document doc, String attrName, float value, boolean indexed) {
    doc.add(new FloatField(attrName, value, indexed ? FloatField.TYPE_STORED : NonIndexedFieldType.FLOAT));
  }

  private static void addEnumField(Document doc, String attrName, String enumStorageString, boolean indexed) {
    doc.add(createField(attrName, enumStorageString, indexed ? StringField.TYPE_STORED : StoredField.TYPE));
  }

  private static void addDoubleField(Document doc, String attrName, double value, boolean indexed) {
    doc.add(new DoubleField(attrName, value, indexed ? DoubleField.TYPE_STORED : NonIndexedFieldType.DOUBLE));
  }

  private static void addSqlDateField(Document doc, String attrName, java.sql.Date value, boolean indexed) {
    addLongField(doc, attrName, value.getTime(), indexed);
  }

  private static void addDateField(Document doc, String attrName, Date value, boolean indexed) {
    addLongField(doc, attrName, value.getTime(), indexed);
  }

  private static void addCharField(Document doc, String attrName, char value, boolean indexed) {
    addIntField(doc, attrName, value, indexed);
  }

  private static void addByteArrayField(Document doc, String attrName, byte[] value, boolean indexed)
      throws IndexException {
    if (indexed) { throw new IndexException("byte array attributes can only be stored (not indexed)"); }
    doc.add(new Field(attrName, value, StoredField.TYPE));
  }

  private static void addByteField(Document doc, String attrName, byte value, boolean indexed) {
    addIntField(doc, attrName, value, indexed);
  }

  private static void addBooleanField(Document doc, String attrName, boolean value, boolean indexed) {
    addIntField(doc, attrName, value ? 1 : 0, indexed);
  }

  private ValueID valueForKey(String key) throws IndexException {
    DirectoryReader indexReader = null;
    try {
      indexReader = getReader();
      IndexSearcher searcher = new IndexSearcher(indexReader);

      Query query = new TermQuery(new Term(KEY_FIELD_NAME, key));

      TopDocs docs = searcher.search(query, 2);
      if (docs.scoreDocs.length > 1) { throw new AssertionError("more than one result for key: " + key); }

      if (docs.scoreDocs.length == 0) { return null; }

      int docId = docs.scoreDocs[0].doc;
      Document doc = indexReader.document(docId, Collections.singleton(VALUE_FIELD_NAME));
      long oid = (Long) getFieldValue(doc, VALUE_FIELD_NAME, ValueType.LONG);
      return new ValueID(oid);
    } catch (IOException e) {
      throw new IndexException(e);
    }
    finally {
      try {
        if (readerMgr != null && indexReader != null) readerMgr.release(indexReader);
      } catch (IOException e) {
        throw new IndexException(e);
      }
    }

  }

  DirectoryReader getReader() throws IOException {
    DirectoryReader reader = readerMgr.acquire();
    DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer, true);
    if (newReader != null) {
        readerMgr.release(reader);
        return newReader;
    } else {
        return reader;
    }
  }
  
  void releaseReader(final DirectoryReader reader) {
      Runnable r = new Runnable () {
          public void run() {
              try {
                  readerMgr.release(reader);
              } catch (IOException io) {
                  logger.info("IO Failed", io);
              }
          }
      };
      if (useCommitThread) {
          s_commitThreadPool.submit(r);
      } else {
          r.run();
      }
  }
  
  DirectoryReader getReader(QueryID qId) throws IOException, IndexException {
    IndexCommit ic = queryCommits.get(qId);
    if (ic == null) throw new IndexException(String.format("Commit point referenced by query id %s not found", qId));
    DirectoryReader r = null;
    try {
      r = getReader(); // "Regular", NRT reader reference
      return DirectoryReader.openIfChanged(r, ic); // This will actually never return null because the old reader is NRT
    } catch (AlreadyClosedException e) {
      throw new IndexException(e);
    }
    finally {
      try {
        if (readerMgr != null && r != null) readerMgr.release(r);
      } catch (IOException e) {
        throw new IndexException(e);
      }
    }
  }

  private static Field createField(String attrName, String value, FieldType type) {
    return new Field(attrName, value, type);
  }

  void close(boolean waitForMerges) throws IndexException {
    // XXX: this is not thread safe with other operations!
    // XXX: in particular the writer/directory close
    if (shutdown.compareAndSet(false, true)) {
      if (useCommitThread) {
        // wait for commit task to finish, if any
        try {
          Future<?> commitResult;

          commitLock.lock();
          commitResult = committer;
          commitLock.unlock();

          if (commitResult != null) commitResult.get();
        } catch (InterruptedException e1) {
          // XXX: set interrupted status?
          throw new IndexException(e1);

        } catch (ExecutionException ex) {
          throw new IndexException(ex);
        }

      }

      try {
        readerMgr.close();
        writer.close(waitForMerges);
        luceneDirectory.close();
      } catch (IOException e) {
        throw new IndexException(e);
      }
    }
  }

  
  private class CommitTask implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      try {
        while (true) {
          final List<ProcessingContext> committed;
          commitLock.lock();

          if (pending.isEmpty()) break;

          committed = pending;
          pending = newPendingList();

          // Maintain lock so that if commit fails, we can null the reference to our future atomically,
          // and not lose txns in addPendingContext
          // On shutdown, we close the writer which performs one final commit, so skip it in that case
          if (!shutdown.get()) writer.commit();
          commitLock.unlock();
          // Do this outside the lock scope
          for (ProcessingContext context : committed) {
            context.processed();
          }
          readerMgr.maybeRefresh();
        }
      }
      finally {
        // If terminating abnormally, lock may not be held
        if (!commitLock.isHeldByCurrentThread()) commitLock.lock();
        committer = null;
        commitLock.unlock();
      }
      return null;
    }
  }

  private static class IndexInputAdapter extends InputStream {

    private final IndexInput input;

    private IndexInputAdapter(IndexInput input) {
      this.input = input;
    }

    @Override
    public int read() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = Math.min(available(), len);
      input.readBytes(b, off, len, false);
      return read;
    }

    @Override
    public int available() {
      long available = input.length() - input.getFilePointer();

      if (available >= Integer.MAX_VALUE) { return Integer.MAX_VALUE; }

      return (int) available;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public long skip(long n) {
      throw new UnsupportedOperationException();
    }

  }

}

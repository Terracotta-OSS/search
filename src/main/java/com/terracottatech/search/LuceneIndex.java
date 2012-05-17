/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.NumericUtils;

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
import com.terracottatech.search.LuceneIndexManager.IndexGroup;
import com.terracottatech.search.aggregator.Aggregator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LuceneIndex {

  private static final ExecutorService     s_commitThreadPool    = Executors.newFixedThreadPool(1);

  static final String                      TERRACOTTA_INIT_FILE  = "__terracotta_init.txt";

  public static final String               KEY_BYTES_FIELD_NAME  = "__TC_KEY_BYTES";
  public static final String               SEGMENT_ID_FIELD_NAME = "__TC_SEGMENT_ID";
  static final String                      KEY_FIELD_NAME        = "__TC_KEY_FIELD";
  static final String                      VALUE_FIELD_NAME      = "__TC_VALUE_FIELD";

  private static final FieldSelector       VALUE_ONLY_SELECTOR   = valueOnlySelector();

  private final Analyzer                   analyzer              = new LowerCaseKeywordAnalyzer();
  private final Directory                  luceneDirectory;
  private final SnapshotDeletionPolicy     snapshotter;
  private final IndexWriter                writer;
  private final File                       path;
  private final String                     name;
  private final AtomicReference<Future<?>> committer             = new AtomicReference<Future<?>>();
  private final AtomicBoolean              shutdown              = new AtomicBoolean();
  private final Logger                     logger;
  private final boolean                    useCommitThread;

  private List<ProcessingContext>          pending               = newPendingList();
  private final IndexGroup                 idxGroup;

  private final AtomicReference<Thread>    accessor              = new AtomicReference<Thread>();

  private boolean                          snapshotTaken         = false;                           // XXX - change
                                                                                                     // to
                                                                                                     // snapshot name
                                                                                                     // when upgrading
                                                                                                     // to Lucene 3.5+

  LuceneIndex(Directory directory, String name, File path, boolean useCommitThread, IndexGroup parent,
              Configuration cfg, LoggerFactory loggerFactory) throws IndexException {
    this.logger = loggerFactory.getLogger(getClass().getName() + "-" + path.getName());
    this.snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    this.path = path;
    this.name = name;
    this.luceneDirectory = directory;
    this.useCommitThread = useCommitThread;
    this.idxGroup = parent;

    try {
      Util.ensureDirectory(path);

      double ramBufferSize = cfg.getMaxRamBufferSize();
      int mergeFactor = cfg.getMergeFactor();
      int maxBufferedDocs = cfg.getMaxBufferedDocs();
      int maxMergeDocs = cfg.getMaxMergeDocs();

      writer = new IndexWriter(luceneDirectory, analyzer, snapshotter, MaxFieldLength.UNLIMITED);
      writer.setRAMBufferSizeMB(ramBufferSize);
      writer.setMaxMergeDocs(maxMergeDocs);
      writer.setMaxBufferedDocs(maxBufferedDocs);
      writer.setMergeFactor(mergeFactor);
      logger.info("Directory type: " + luceneDirectory.getClass().getName() + " with max buffer size " + ramBufferSize
                  + " and merge factor " + mergeFactor + " ram buffer " + writer.getRAMBufferSizeMB()
                  + " maxBufferedDocs: " + writer.getMaxBufferedDocs() + " maxMergeDocs: " + maxMergeDocs);

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

  private static FieldSelector valueOnlySelector() {
    return new SetBasedFieldSelector(Collections.singleton(VALUE_FIELD_NAME), Collections.EMPTY_SET);
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

    return new IndexInputAdapter(luceneDirectory.openInput(fileName));
  }

  void optimize() throws CorruptIndexException, IOException {
    writer.optimize();
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

  synchronized List<IndexFile> getSnapshot() throws IndexException {
    // This method synchronized to be exclusive with release()
    // *AND* to be able to safely capture a snapshot of our schema file

    try {
      writer.commit();
    } catch (IOException ioe) {
      logger.error(ioe);
      throw new IndexException(ioe);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Taking snapshot for index: " + name);
    }

    try {
      List<IndexFile> files = new ArrayList();

      // include the init file so caches on the other end won't be immediately deleted upon open :-)
      files.add(new IndexFileImpl(TERRACOTTA_INIT_FILE, TERRACOTTA_INIT_FILE, path.getName(), true,
                                  new File(path, TERRACOTTA_INIT_FILE).length()));

      // Add lucene files last (ie. after TC special files)
      IndexCommit ic = this.snapshotter.snapshot();
      Collection<String> fileNames = ic.getFileNames();
      for (String fileName : fileNames) {
        if (!fileName.endsWith(".lock")) {
          files.add(new IndexFileImpl(fileName, fileName, path.getName(), false, luceneDirectory.fileLength(fileName)));
        }
      }

      snapshotTaken = true;

      return files;
    } catch (Exception e) {
      // XXX: this seems a tad extreme
      throw new AssertionError(e);
    }
  }

  synchronized void release() {
    if (!snapshotTaken) return;
    if (logger.isDebugEnabled()) {
      logger.info("Releasing snapshot for index");
    }
    snapshotTaken = false;

    snapshotter.release();
  }

  private void addPendingContext(ProcessingContext context) {
    if (useCommitThread) {
      synchronized (this) {
        pending.add(context);
      }
      committer.compareAndSet(null, s_commitThreadPool.submit(new CommitTask()));

    } else {
      context.processed();
    }
  }

  SearchResult search(List queryStack, boolean includeKeys, boolean includeValues, Set<String> attributeSet,
                      Set<String> groupByAttributes, List<NVPair> sortAttributes, int maxResults, boolean incCount)
      throws IndexException {

    IndexReader indexReader = null;
    try {
      Query luceneQuery = new LuceneQueryBuilder(queryStack, idxGroup.getSchema()).buildQuery();

      indexReader = getNewReader();

      IndexSearcher searcher = new IndexSearcher(indexReader);

      final DocIdList docIds;

      boolean isGroupBy = !groupByAttributes.isEmpty();

      if (isGroupBy) maxResults = -1; // unbounded if doing a grouped search - to be limited higher
                                      // up

      if (!isGroupBy && sortAttributes.size() > 0) {
        if (maxResults == 0) {
          docIds = new EmptyDocIdList();
        } else {
          // first count total hits without sorting so we can pass
          // a reasonable "nDocs" count for the sorted search
          TotalHitCountCollector collector = new TotalHitCountCollector();
          searcher.search(luceneQuery, collector);

          int nDocs = maxResults > 0 ? Math.min(maxResults, collector.getTotalHits()) : collector.getTotalHits();
          if (nDocs > 0) {
            Sort sort = getSort(sortAttributes);
            TopFieldDocs topDocs = searcher.search(luceneQuery, null, nDocs, sort);
            docIds = new TopDocsIds(topDocs);
          } else {
            docIds = new EmptyDocIdList();
          }
        }
      } else {
        SimpleCollector collector = new SimpleCollector(maxResults);
        searcher.search(luceneQuery, collector);
        docIds = collector;
      }

      List<IndexQueryResult> results = new ArrayList<IndexQueryResult>();

      for (int i = 0, n = docIds.size(); i < n; i++) {
        Document doc = indexReader.document(docIds.get(i));
        String key = includeKeys ? doc.get(KEY_FIELD_NAME) : null;
        ValueID value = includeValues ? new ValueID(Long.parseLong(doc.get(VALUE_FIELD_NAME))) : ValueID.NULL_ID;

        final List<NVPair> attributes = attributeSet.isEmpty() ? Collections.EMPTY_LIST
            : new ArrayList<NVPair>(attributeSet.size());
        Set<NVPair> groupByAttrs = isGroupBy ? new HashSet<NVPair>(groupByAttributes.size()) : Collections.EMPTY_SET;
        List<NVPair> sortAttributesList = sortAttributes.isEmpty() ? Collections.EMPTY_LIST
            : new ArrayList<NVPair>(sortAttributes.size());

        for (String attrKey : attributeSet) {
          // XXX: type lookup can be done up front
          ValueType type = idxGroup.getSchema().get(attrKey);

          Object attrValue = getFieldValue(doc, attrKey, type);
          attributes.add(AbstractNVPair.createNVPair(attrKey, attrValue, type));
        }

        for (String attrKey : groupByAttributes) {
          ValueType type = idxGroup.getSchema().get(attrKey);
          Object attrValue = getFieldValue(doc, attrKey, type);
          groupByAttrs.add(AbstractNVPair.createNVPair(attrKey, attrValue, type));
        }

        // Skip adding this as a result unless needed
        if (includeKeys || includeValues || incCount || !attributes.isEmpty()) {

          for (NVPair pair : sortAttributes) {
            String sortAttrKey = pair.getName();
            ValueType type = idxGroup.getSchema().get(sortAttrKey);
            Object attrValue = getFieldValue(doc, sortAttrKey, type);
            NVPair attributePair = AbstractNVPair.createNVPair(sortAttrKey, attrValue, type);
            sortAttributesList.add(attributePair);
          }

          results.add(isGroupBy ? new GroupedIndexQueryResultImpl(attributes, sortAttributesList, groupByAttrs,
                                                                  new ArrayList<Aggregator>())
              : new NonGroupedIndexQueryResultImpl(key, value, attributes, sortAttributesList));
        }
      }

      return new SearchResult(results, Collections.EMPTY_LIST /* placeholder for aggregators */, docIds.size() > 0);
    } catch (Exception e) {
      // XXX: This catch is perhaps too broad

      if (e instanceof IndexException) { throw (IndexException) e; }
      throw new IndexException(e);
    } finally {
      if (indexReader != null) {
        try {
          indexReader.close();
        } catch (IOException e) {
          logger.error(e);
          // XXX: do we need to take more action on this exception?
        }
      }
    }
  }

  private Object getFieldValue(Document doc, String attrKey, ValueType type) {
    if (type == ValueType.BYTE_ARRAY) { return doc.getBinaryValue(attrKey); }

    String attrValue = doc.get(attrKey);
    if (attrValue == null) return null;

    switch (type) {
      case BOOLEAN:
        int value = Integer.valueOf(attrValue);
        if (value == 0) {
          return Boolean.FALSE;
        } else {
          return Boolean.TRUE;
        }
      case BYTE:
        return Byte.valueOf(attrValue);
      case BYTE_ARRAY:
        // Should have been handled above
        throw new AssertionError(type);
      case CHAR:
        return new Character((char) Integer.valueOf(attrValue).intValue());
      case DATE:
        return new Date(Long.valueOf(attrValue));
      case SQL_DATE:
        return new java.sql.Date(Long.valueOf(attrValue));
      case DOUBLE:
        return Double.valueOf(attrValue);
      case ENUM:
        return attrValue;
      case FLOAT:
        return Float.valueOf(attrValue);
      case INT:
        return Integer.valueOf(attrValue);
      case LONG:
        return Long.valueOf(attrValue);
      case SHORT:
        return Short.valueOf(attrValue);
      case STRING:
        return attrValue;
      case NULL:
        throw new AssertionError();
      case VALUE_ID:
        return new ValueID(Long.valueOf(attrValue));
    }

    throw new AssertionError(type);
  }

  void remove(String key, ProcessingContext context) throws IndexException {
    if (!accessor.compareAndSet(null, Thread.currentThread()) && accessor.get() != Thread.currentThread()) { throw new AssertionError(
                                                                                                                                      "Index is being accessed by a different thread"); }

    try {
      writer.deleteDocuments(new Term(KEY_FIELD_NAME, key));
    } catch (Exception e) {
      context.processed();
      throw new IndexException(e);
    }

    addPendingContext(context);
  }

  void removeIfValueEqual(Map<String, ValueID> toRemove, ProcessingContext context) throws IndexException {
    IndexReader reader = null;

    try {
      reader = getNewReader();
      IndexSearcher searcher = new IndexSearcher(reader);

      for (Entry<String, ValueID> e : toRemove.entrySet()) {
        String stringKey = e.getKey();
        ValueID oidValue = e.getValue();

        removeIfValueEqual(stringKey, oidValue, reader, searcher);
      }

      addPendingContext(context);
    } catch (Exception e) {
      context.processed();
      throw new IndexException(e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ioe) {
          logger.error(ioe);
        }
      }
    }
  }

  private void removeIfValueEqual(String key, ValueID value, IndexReader reader, IndexSearcher searcher)
      throws IOException {
    if (!accessor.compareAndSet(null, Thread.currentThread()) && accessor.get() != Thread.currentThread()) { throw new AssertionError(
                                                                                                                                      "Index is being accessed by a different thread"); }

    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term(KEY_FIELD_NAME, key)), Occur.MUST));
    query
        .add(new BooleanClause(
                               new TermQuery(new Term(VALUE_FIELD_NAME, NumericUtils.longToPrefixCoded(value.toLong()))),
                               Occur.MUST));
    writer.deleteDocuments(query);
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

  void replaceIfPresent(String key, ValueID value, Object oldValue, List<NVPair> attributes,
                        List<NVPair> storeOnlyAttributes, long segmentOid, ProcessingContext context)
      throws IndexException {
    try {
      ValueID existingValue = valueForKey(key);
      if (existingValue == null || !existingValue.equals(oldValue)) {
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

  public void update(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                     long segmentOid, ProcessingContext context) throws IndexException {
    try {
      upsertInternal(key, value, attributes, storeOnlyAttributes, segmentOid, false);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  public void insert(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                     long segmentOid, ProcessingContext context) throws IndexException {
    try {
      upsertInternal(key, value, attributes, storeOnlyAttributes, segmentOid, true);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  public void updateKey(String existingKey, String newKey, int segmentId, ProcessingContext context)
      throws IndexException {
    try {
      updateKeyInternal(existingKey, newKey);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  private void updateKeyInternal(String existingKey, String newKey) throws IndexException {
    IndexReader indexReader = null;
    try {
      indexReader = getNewReader();
      IndexSearcher searcher = new IndexSearcher(indexReader);

      Query query = new TermQuery(new Term(KEY_FIELD_NAME, existingKey));

      TopDocs docs = searcher.search(query, 2);
      if (docs.scoreDocs.length > 1) { throw new AssertionError("more than one result for key: " + existingKey); }
      if (docs.scoreDocs.length == 0) { throw new IndexException("No such document for key: " + existingKey); }

      int docId = docs.scoreDocs[0].doc;
      Document oldDoc = indexReader.document(docId);
      Document newDoc = new Document();
      addKeyField(newDoc, newKey);

      for (Fieldable f : oldDoc.getFields()) {
        String fieldName = f.name();
        if (!fieldName.equals(KEY_FIELD_NAME)) {
          ValueType fieldType = idxGroup.getSchema().get(fieldName);
          if (fieldType == null) { throw new AssertionError("missing type for field " + fieldName); }
          Object value = getFieldValue(oldDoc, fieldName, fieldType);
          addField(newDoc, fieldName, value, fieldType, f.isIndexed());
        }
      }

      writer.updateDocument(new Term(KEY_FIELD_NAME, existingKey), newDoc);
    } catch (IOException e) {
      throw new IndexException(e);
    } finally {
      if (indexReader != null) {
        try {
          indexReader.close();
        } catch (IOException e) {
          // XXX: do we need to take more action on this exception?
          logger.error(e);
        }
      }
    }
  }

  private void addField(Document doc, String fieldName, Object value, ValueType fieldType, boolean indexed)
      throws IndexException {
    switch (fieldType) {
      case BOOLEAN: {
        addBooleanField(doc, fieldName, (Boolean) value, indexed);
        return;
      }
      case BYTE: {
        addByteField(doc, fieldName, (Byte) value, indexed);
        return;
      }
      case BYTE_ARRAY: {
        addByteArrayField(doc, fieldName, (byte[]) value, indexed);
        return;
      }
      case CHAR: {
        addCharField(doc, fieldName, (Character) value, indexed);
        return;
      }
      case DATE: {
        addDateField(doc, fieldName, (Date) value, indexed);
        return;
      }
      case SQL_DATE: {
        addSqlDateField(doc, fieldName, (java.sql.Date) value, indexed);
        return;
      }
      case DOUBLE: {
        addDoubleField(doc, fieldName, (Double) value, indexed);
        return;
      }
      case ENUM: {
        addEnumField(doc, fieldName, (String) value, indexed);
        return;
      }
      case FLOAT: {
        addFloatField(doc, fieldName, (Float) value, indexed);
        return;
      }
      case INT: {
        addIntField(doc, fieldName, (Integer) value, indexed);
        return;
      }
      case LONG: {
        addLongField(doc, fieldName, (Long) value, indexed);
        return;
      }
      case NULL: {
        throw new AssertionError();
      }
      case SHORT: {
        addShortField(doc, fieldName, (Short) value, indexed);
        return;
      }
      case STRING: {
        addStringField(doc, fieldName, (String) value, indexed);
        return;
      }
      case VALUE_ID: {
        addValueIdField(doc, fieldName, (ValueID) value, indexed);
        return;
      }
    }

    throw new AssertionError(fieldType);

  }

  private void upsertInternal(String key, ValueID value, List<NVPair> attributes, List<NVPair> storeOnlyAttributes,
                              long segmentOid, boolean isInsert) throws IndexException {
    if (!accessor.compareAndSet(null, Thread.currentThread()) && accessor.get() != Thread.currentThread()) {
      //
      throw new AssertionError("Index is being accessed by a different thread");
    }

    idxGroup.checkSchema(attributes);
    idxGroup.checkSchema(storeOnlyAttributes);

    Document doc = new Document();

    addKeyField(doc, key);

    doc.add(createNumericField(VALUE_FIELD_NAME, true).setLongValue(value.toLong()));

    // this field is used to implement per-CDSM segment clearing
    doc.add(createNumericField(SEGMENT_ID_FIELD_NAME, true).setLongValue(segmentOid));

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

  private static void addKeyField(Document doc, String key) {
    doc.add(new Field(KEY_FIELD_NAME, key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
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
          addValueIdField(doc, attrName, oidNVPair.getValue(), indexed);
          continue;
      }

      throw new AssertionError(type.name());
    }
  }

  private static void addValueIdField(Document doc, String fieldName, ValueID value, boolean indexed) {
    doc.add(createNumericField(fieldName, indexed).setLongValue(value.toLong()));
  }

  private static void addStringField(Document doc, String fieldName, String value, boolean indexed) {
    doc.add(createField(fieldName, value, indexed ? Field.Index.ANALYZED_NO_NORMS : Field.Index.NO));
  }

  private static void addShortField(Document doc, String fieldName, short value, boolean indexed) {
    doc.add(createNumericField(fieldName, indexed).setIntValue(value));
  }

  private static void addLongField(Document doc, String fieldName, long value, boolean indexed) {
    doc.add(createNumericField(fieldName, indexed).setLongValue(value));
  }

  private static void addIntField(Document doc, String attrName, int value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setIntValue(value));
  }

  private static void addFloatField(Document doc, String attrName, float value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setFloatValue(value));
  }

  private static void addEnumField(Document doc, String attrName, String enumStorageString, boolean indexed) {
    doc.add(createField(attrName, enumStorageString, indexed ? Field.Index.NOT_ANALYZED_NO_NORMS : Field.Index.NO));
  }

  private static void addDoubleField(Document doc, String attrName, double value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setDoubleValue(value));
  }

  private static void addSqlDateField(Document doc, String attrName, java.sql.Date value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setLongValue(value.getTime()));
  }

  private static void addDateField(Document doc, String attrName, Date value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setLongValue(value.getTime()));
  }

  private static void addCharField(Document doc, String attrName, char value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setIntValue(value));
  }

  private static void addByteArrayField(Document doc, String attrName, byte[] value, boolean indexed)
      throws IndexException {
    if (indexed) { throw new IndexException("byte array attributes can only be stored (not indexed)"); }
    doc.add(new Field(attrName, value, Field.Store.YES));
  }

  private static void addByteField(Document doc, String attrName, byte value, boolean indexed) {
    doc.add(createNumericField(attrName, indexed).setIntValue(value));
  }

  private static void addBooleanField(Document doc, String attrName, boolean value, boolean indexed) {
    if (value) {
      doc.add(createNumericField(attrName, indexed).setIntValue(1));
    } else {
      doc.add(createNumericField(attrName, indexed).setIntValue(0));
    }
  }

  private ValueID valueForKey(String key) throws IndexException {
    IndexReader indexReader = null;
    try {
      indexReader = getNewReader();
      IndexSearcher searcher = new IndexSearcher(indexReader);

      Query query = new TermQuery(new Term(KEY_FIELD_NAME, key));

      TopDocs docs = searcher.search(query, 2);
      if (docs.scoreDocs.length > 1) { throw new AssertionError("more than one result for key: " + key); }

      if (docs.scoreDocs.length == 0) { return null; }

      int docId = docs.scoreDocs[0].doc;
      Document doc = indexReader.document(docId, VALUE_ONLY_SELECTOR);
      long oid = (Long) getFieldValue(doc, VALUE_FIELD_NAME, ValueType.LONG);
      return new ValueID(oid);
    } catch (IOException e) {
      throw new IndexException(e);
    } finally {
      if (indexReader != null) {
        try {
          indexReader.close();
        } catch (IOException e) {
          // XXX: do we need to take more action on this exception?
          logger.error(e);
        }
      }
    }
  }

  private IndexReader getNewReader() throws CorruptIndexException, IOException {
    // XXX: look into whether we can pass false "apply deletes" here (for newer lucene versions)
    // return IndexReader.open(writer, true);

    return writer.getReader();
  }

  private Sort getSort(List<NVPair> sortAttributes) throws IndexException {
    List<SortField> sortFields = new ArrayList<SortField>(sortAttributes.size() * 2);
    for (NVPair sortAttributePair : sortAttributes) {
      String attributeName = sortAttributePair.getName();
      Boolean descending = SortOperations.DESCENDING.equals(sortAttributePair.getObjectValue()) ? true : false;
      sortFields.add(new SortField(attributeName, getSortFieldType(attributeName), descending));
    }
    Sort sort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));
    return sort;
  }

  private static Field createField(String attrName, String value, Field.Index index) {
    return new Field(attrName, value, Field.Store.YES, index);
  }

  private static NumericField createNumericField(String attrName, boolean indexed) {
    return new NumericField(attrName, Field.Store.YES, indexed);
  }

  void close() {
    // XXX: this is not thread safe with other operations!
    // XXX: in particular the writer/directory close
    // XXX: and the commit thread not finish all pending commits!

    if (shutdown.compareAndSet(false, true)) {
      synchronized (this) {
        // notify commit thread
        notify();
      }

      if (useCommitThread) {
        try {
          Future<?> commitResult = committer.get();
          if (commitResult != null) commitResult.get();
        } catch (InterruptedException e1) {
          // XXX: set interrupted status?
        } catch (ExecutionException ex) {
          logger.warn(ex);
        }
      }

      try {
        writer.close();
      } catch (Exception e) {
        logger.error(e);
      }

      try {
        luceneDirectory.close();
      } catch (IOException e) {
        logger.error(e);
      }
    }
  }

  private int getSortFieldType(String attributeName) throws IndexException {
    ValueType valueType = idxGroup.getSchema().get(attributeName);
    if (valueType == null) { throw new IndexException("no such attribute named [" + attributeName + "]"); }

    switch (valueType) {
      case BOOLEAN:
        return SortField.INT;
      case BYTE:
        return SortField.INT;
      case BYTE_ARRAY:
        throw new IndexException("Unexpected sort type (" + valueType.name() + "] for attribute " + attributeName);
      case CHAR:
        return SortField.INT;
      case DATE:
        return SortField.LONG;
      case SQL_DATE:
        return SortField.LONG;
      case DOUBLE:
        return SortField.DOUBLE;
      case ENUM:
        return SortField.STRING;
      case FLOAT:
        return SortField.FLOAT;
      case INT:
        return SortField.INT;
      case LONG:
        return SortField.LONG;
      case SHORT:
        return SortField.INT;
      case STRING:
        return SortField.STRING;
      case NULL:
        throw new AssertionError();
      case VALUE_ID:
        return SortField.LONG;
    }

    throw new AssertionError("unexpected sort type: " + valueType);

  }

  private class CommitTask implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          final List<ProcessingContext> committed;
          synchronized (LuceneIndex.this) {
            if (pending.isEmpty() || shutdown.get()) return;

            committed = pending;
            pending = newPendingList();
          }

          try {
            writer.commit();
          } catch (Exception e) {
            logger.error(e);
            // XXX: do we need to take further action here?
          }

          for (ProcessingContext context : committed) {
            context.processed();
          }
        }
      } finally {
        committer.set(null);
      }
    }
  }

  private interface DocIdList {
    int size();

    int get(int index);
  }

  private static class EmptyDocIdList implements DocIdList {

    public int size() {
      return 0;
    }

    public int get(int index) {
      throw new NoSuchElementException("index: " + index);
    }

  }

  private static class TopDocsIds implements DocIdList {

    private final ScoreDoc[] scoreDocs;

    TopDocsIds(TopDocs topDocs) {
      scoreDocs = topDocs.scoreDocs;
    }

    public int size() {
      return scoreDocs.length;
    }

    public int get(int index) {
      return scoreDocs[index].doc;
    }

  }

  private static class SimpleCollector extends Collector implements DocIdList {

    private final int     maxResults;
    private final IntList ids  = new IntList();
    private final boolean unbounded;
    private int           base = 0;

    private SimpleCollector(int maxResults) {
      this.maxResults = maxResults;
      this.unbounded = maxResults < 0;
    }

    @Override
    public void setScorer(Scorer scorer) {
      //
    }

    @Override
    public void collect(int doc) {
      if (unbounded || ids.size() < maxResults) {
        ids.add(base + doc);
      }
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) {
      base = docBase;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

    public int size() {
      return ids.size();
    }

    public int get(int index) {
      return ids.get(index);
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

  private static class IntList {

    private int   size = 0;
    private int[] data;

    IntList() {
      this(16);
    }

    IntList(int cap) {
      data = new int[cap];
    }

    int size() {
      return size;
    }

    void add(int toAdd) {
      if (size == data.length) {
        int[] temp = new int[data.length * 2];
        System.arraycopy(data, 0, temp, 0, data.length);
        data = temp;
      }

      data[size++] = toAdd;

    }

    int get(int index) {
      return data[index];
    }

  }

}

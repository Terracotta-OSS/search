/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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

class LuceneIndex {

  private static final ExecutorService     s_commitThreadPool    = Executors.newFixedThreadPool(1);

  static final String                      TERRACOTTA_INIT_FILE  = "__terracotta_init.txt";

  static final String                      KEY_FIELD_NAME        = "__TC_KEY_FIELD";
  static final String                      VALUE_FIELD_NAME      = "__TC_VALUE_FIELD";
  static final String                      SEGMENT_ID_FIELD_NAME = "__TC_SEGMENT_ID";

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

  private boolean                          snapshotTaken         = false;                           // XXX - change to
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
                      List<NVPair> sortAttributes, int maxResults, boolean incCount) throws IndexException {

    IndexReader indexReader = null;
    try {
      Query luceneQuery = new LuceneQueryBuilder(queryStack, idxGroup.getSchema()).buildQuery();

      indexReader = getNewReader();

      IndexSearcher searcher = new IndexSearcher(indexReader);

      final DocIdList docIds;

      if (sortAttributes.size() > 0) {
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

        List<NVPair> attributes = attributeSet.isEmpty() ? Collections.EMPTY_LIST : new ArrayList();
        List<NVPair> sortAttributesList = sortAttributes.isEmpty() ? Collections.EMPTY_LIST : new ArrayList();

        for (String attrKey : attributeSet) {
          // XXX: type lookup can be done up front
          ValueType type = idxGroup.getSchema().get(attrKey);
          Object attrValue = getFieldValue(doc, attrKey, type);
          if (attrValue != null) {
            attributes.add(AbstractNVPair.createNVPair(attrKey, attrValue, type));
          }
        }

        // Skip adding this as a result unless needed
        if (includeKeys || includeValues || incCount || !attributes.isEmpty()) {

          for (NVPair pair : sortAttributes) {
            String sortAttrKey = pair.getName();
            ValueType type = idxGroup.getSchema().get(sortAttrKey);
            Object attrValue = getFieldValue(doc, sortAttrKey, type);
            if (attrValue != null) {
              NVPair attributePair = AbstractNVPair.createNVPair(sortAttrKey, attrValue, type);
              sortAttributesList.add(attributePair);
            }
          }

          results.add(new IndexQueryResultImpl(key, value, attributes, sortAttributesList));
        }
      }

      return new SearchResult(results, Collections.EMPTY_LIST /* placeholder for aggregators */, docIds.size() > 0);
    } catch (Exception e) {
      // XXX: This is perhaps too broad
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
        // XXX: this is an unexpected type!
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

  void replaceIfPresent(String key, ValueID value, Object oldValue, List<NVPair> attributes, long segmentOid,
                        ProcessingContext context) throws IndexException {
    try {
      ValueID existingValue = valueForKey(key);
      if (existingValue == null || !existingValue.equals(oldValue)) {
        context.processed();
        return;
      }

      upsertInternal(key, value, attributes, segmentOid, false);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  public void update(String key, ValueID value, List<NVPair> attributes, long segmentOid, ProcessingContext context)
      throws IndexException {
    try {
      upsertInternal(key, value, attributes, segmentOid, false);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }
  }

  public void insert(String key, ValueID value, List<NVPair> attributes, long segmentOid, ProcessingContext context)
      throws IndexException {
    try {
      upsertInternal(key, value, attributes, segmentOid, true);
      addPendingContext(context);
    } catch (IndexException ie) {
      context.processed();
      throw ie;
    }

  }

  private void upsertInternal(String key, ValueID value, List<NVPair> attributes, long segmentOid, boolean isInsert)
      throws IndexException {
    if (!accessor.compareAndSet(null, Thread.currentThread()) && accessor.get() != Thread.currentThread()) { throw new AssertionError(
                                                                                                                                      "Index is being accessed by a different thread"); }

    idxGroup.checkSchema(attributes);

    Document doc = new Document();

    doc.add(new Field(KEY_FIELD_NAME, key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

    doc.add(createNumericField(VALUE_FIELD_NAME).setLongValue(value.toLong()));

    // this field is used to implement per-CDSM segment clearing
    doc.add(createNumericField(SEGMENT_ID_FIELD_NAME).setLongValue(segmentOid));

    for (NVPair nvpair : attributes) {
      String attrName = nvpair.getName();

      ValueType type = nvpair.getType();
      switch (type) {
        case BOOLEAN:
          BooleanNVPair booleanPair = (BooleanNVPair) nvpair;
          if (booleanPair.getValue()) {
            doc.add(createNumericField(attrName).setIntValue(1));
          } else {
            doc.add(createNumericField(attrName).setIntValue(0));
          }
          break;
        case BYTE:
          ByteNVPair bytePair = (ByteNVPair) nvpair;
          doc.add(createNumericField(attrName).setIntValue(bytePair.getValue()));
          break;
        case BYTE_ARRAY:
          throw new IndexException("We do not index byte array's. Attribute name: " + attrName);
        case CHAR:
          CharNVPair charPair = (CharNVPair) nvpair;
          doc.add(createNumericField(attrName).setIntValue(charPair.getValue()));
          break;
        case DATE:
          DateNVPair datePair = (DateNVPair) nvpair;
          doc.add(createNumericField(attrName).setLongValue(datePair.getValue().getTime()));
          break;
        case SQL_DATE:
          SqlDateNVPair sqlDatePair = (SqlDateNVPair) nvpair;
          doc.add(createNumericField(attrName).setLongValue(sqlDatePair.getValue().getTime()));
          break;
        case DOUBLE:
          DoubleNVPair doubleNVPair = (DoubleNVPair) nvpair;
          doc.add(createNumericField(attrName).setDoubleValue(doubleNVPair.getValue()));
          break;
        case ENUM:
          EnumNVPair enumPair = (EnumNVPair) nvpair;
          doc.add(createField(attrName, AbstractNVPair.enumStorageString(enumPair), Field.Index.NOT_ANALYZED_NO_NORMS));
          break;
        case FLOAT:
          FloatNVPair floatNVPair = (FloatNVPair) nvpair;
          doc.add(createNumericField(attrName).setFloatValue(floatNVPair.getValue()));
          break;
        case INT:
          IntNVPair intNVPair = (IntNVPair) nvpair;
          doc.add(createNumericField(attrName).setIntValue(intNVPair.getValue()));
          break;
        case LONG:
          LongNVPair longNVPair = (LongNVPair) nvpair;
          doc.add(createNumericField(attrName).setLongValue(longNVPair.getValue()));
          break;
        case SHORT:
          ShortNVPair shortNVPair = (ShortNVPair) nvpair;
          doc.add(createNumericField(attrName).setIntValue(shortNVPair.getValue()));
          break;
        case STRING:
          StringNVPair stringNVPair = (StringNVPair) nvpair;
          doc.add(createField(attrName, stringNVPair.getValue(), Field.Index.ANALYZED_NO_NORMS));
          break;
        case NULL:
          throw new AssertionError();
        case VALUE_ID:
          ValueIdNVPair oidNVPair = (ValueIdNVPair) nvpair;
          doc.add(createNumericField(attrName).setLongValue(oidNVPair.getValue().toLong()));
          break;
      }
    }

    try {
      if (isInsert) {
        writer.addDocument(doc);
      } else {
        writer.updateDocument(new Term(KEY_FIELD_NAME, key), doc);
      }

    } catch (Exception e) {
      throw new IndexException(e);
    }

    // XXX: We'll want to optimize() the index occasionally
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

  private Sort getSort(List<NVPair> sortAttributes) {
    List<SortField> sortFields = new ArrayList<SortField>(sortAttributes.size() * 2);
    for (NVPair sortAttributePair : sortAttributes) {
      String attributeName = sortAttributePair.getName();
      Boolean descending = SortOperations.DESCENDING.equals(sortAttributePair.getObjectValue()) ? true : false;
      sortFields.add(new SortField(attributeName, getSortFieldType(attributeName), descending));
    }
    Sort sort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));
    return sort;
  }

  private Field createField(String attrName, String value, Field.Index index) {
    return new Field(attrName, value, Field.Store.YES, index);
  }

  private NumericField createNumericField(String attrName) {
    return new NumericField(attrName, Field.Store.YES, true);
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

  private int getSortFieldType(String attributeName) {
    ValueType valueType = idxGroup.getSchema().get(attributeName);

    switch (valueType) {
      case BOOLEAN:
        return SortField.INT;
      case BYTE:
        return SortField.INT;
      case BYTE_ARRAY:
        // XXX: unexpected type!
        throw new AssertionError(valueType);
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

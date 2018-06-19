/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.AbstractNVPair.EnumNVPair;
import com.terracottatech.search.LuceneIndexManager.AttributeProperties;
import com.terracottatech.search.SearchBuilder.Search;
import com.terracottatech.search.aggregator.AbstractAggregator;
import com.terracottatech.search.aggregator.Aggregator;
import com.terracottatech.search.aggregator.Count;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.terracottatech.search.LuceneIndex.getFieldValue;

final class SearchResultsManager {
  
  private final IndexOwner    parent;
  private final SearchResultSourceFactory             pagedResultSourceFactory;
  private final int                                   maxOpenCursors;
  private final AtomicBoolean                         shutdown              = new AtomicBoolean();
  private final Logger                                log;
  private final ExecutorService                       execSvc;
   
  private static final String                         COUNT_AGG_NAME = "__TC_AGG_COUNT"
                                                                       + SearchResultsManager.class.hashCode();

  private final ConcurrentMap<QueryID, SearchResultSource> resultSources  = new ConcurrentHashMap<QueryID, SearchResultSource>();
  private final SearchMonitor searchMonitor;
  private final String parentName;

  private interface ResultsFilter {
    /**
     * @return true if result for query id is to be kept, false otherwise.
     */
    boolean accept(QueryID resId);
  }
  
  SearchResultsManager(File dataPath, final IndexOwner owner, Configuration globalConfig, ExecutorService svc,
                       LoggerFactory loggerFactory, SearchMonitor searchMonitor) {
    this.searchMonitor = searchMonitor;
    this.parentName = owner.getName() == null ? "unknown" : owner.getName();

    pagedResultSourceFactory = new SearchResultSourceFactory(globalConfig, dataPath).setLoggerFactory(loggerFactory)
        .setIndexOwner(new IndexOwner() {

          @Override
          public Map<String, AttributeProperties> getSchema() {
            return Collections.emptyMap(); // XXX - revisit
          }

          @Override
          public String getName() {
            return parent.getName();
          }

          @Override
          public Timer getReaderRefreshTimer() {
            return owner.getReaderRefreshTimer();
          }

          @Override
          public void checkSchema(List<NVPair> attributes, boolean indexed) {
            // no-op
          }
        });

    maxOpenCursors = globalConfig.getMaxOpenResultSets();
    parent = owner;
    log = loggerFactory.getLogger(SearchResultsManager.class);
    execSvc = svc;
  }
  
  void shutdown() {
    if (shutdown.compareAndSet(false, true)) {
      for (SearchResultSource resSrc: resultSources.values()) {
        try {
          resSrc.close();
        }
        catch (IOException e) {
          log.error("Error closing result source", e);
        }
      }
      resultSources.clear();
    }
  }
  
  private static List<NVPair> loadSortFields(Document doc, QueryInputs inputs) {
    Map<String, ValueType> types = inputs.getFieldSchema();
    List<NVPair> sortParams = inputs.getSortAttributes();
    List<NVPair> sortFields = new ArrayList<NVPair>(sortParams.size());
    for (NVPair pair : sortParams) {
      String sortAttrKey = pair.getName();
      ValueType type = types.get(sortAttrKey);
      Object attrValue = getFieldValue(doc, sortAttrKey, type);
      NVPair attributePair = AbstractNVPair.createNVPair(sortAttrKey, attrValue, type);
      sortFields.add(attributePair);
    }

    return sortFields;
  }
  

  static void loadDocumentData(Document doc, QueryInputs searchParams, List<NVPair> attributeValues,
                        Set<NVPair> groupByValues, List<NVPair> sortValues) {
    Map<String, ValueType> types = searchParams.getFieldSchema();
    for (String attrKey : searchParams.getAttributes()) {
      ValueType type = types.get(attrKey);

      Object attrValue = getFieldValue(doc, attrKey, type);
      attributeValues.add(AbstractNVPair.createNVPair(attrKey, attrValue, type));
    }

    for (String attrKey : searchParams.getGroupByAttributes()) {
      ValueType type = types.get(attrKey);
      Object attrValue = getFieldValue(doc, attrKey, type);
      groupByValues.add(AbstractNVPair.createNVPair(attrKey, attrValue, type));
    }

    sortValues.addAll(loadSortFields(doc, searchParams));
  }

  void clearResultsFor(final long requesterId) throws IndexException {
    cleanupResults(new ResultsFilter() {
      
      @Override
      public boolean accept(QueryID resId) {
        return requesterId != resId.requesterId;
      }
    });
    
  }

  void pruneResults(final Set<Long> activeClients) throws IndexException {
    cleanupResults(new ResultsFilter() {
      
      @Override
      public boolean accept(QueryID resId) {
        return activeClients.contains(resId.requesterId);
      }
    });
  }
  
  void resultsProcessed(QueryID id) throws IndexException {
    SearchResultSource results = resultSources.remove(id);
    searchMonitor.finishTrackingQuery(id);
    // Valid scenario if result set existed at some point but was never cached
    if (results == null || results == SearchResultSource.NULL_SOURCE) {
      return;
    }
    try {
      results.close();
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  SearchResult loadResults(long requesterId, long queryId, int offset, int batchLimit) throws IndexException {
    if (shutdown.get()) throw new IndexException("Already closed.");

    QueryID id = new QueryID(requesterId, queryId);
    SearchResultSource src = resultSources.get(id);
    if (src == null) return null;
    if (src == SearchResultSource.NULL_SOURCE)
      // results are not yet available
      throw new IndexException(String.format("Results for query %s not (yet) available", id));

    return src.getResults(offset, batchLimit);
  }

  SearchResult executeQuery(QueryID id, List queryStack, IndexReader[] readers,
                             final boolean includeKeys, final boolean includeValues, Set<String> attributeSet,
                             final Set<String> groupByAttributes, final List<NVPair> sortAttributes,
                            List<NVPair> aggPairs, int resultLimit, int batchLimit) throws IndexException {
    if (shutdown.get()) throw new IndexException("Already closed.");


    boolean isSorted = !sortAttributes.isEmpty();

    boolean isPagedSearch = batchLimit != Search.BATCH_SIZE_UNLIMITED;

    // Disallow concurrent query execution by putting a sentinel value
    @SuppressWarnings("resource")
    SearchResultSource old = isPagedSearch ? resultSources.putIfAbsent(id, SearchResultSource.NULL_SOURCE) : null;
    if (old != null)
      throw new IndexException("Query id already in use: " + id);

    final CustomMultiReader dataReader = new CustomMultiReader(readers);
    try {

      if (isPagedSearch && maxOpenCursors > 0 && resultSources.size() > maxOpenCursors)
        throw new IndexException(String.format("Max open cursor limit reached: %d, offending query: %s", maxOpenCursors, id));

      int queryStackSize=queryStack.size();
      final Query searchQuery = new LuceneQueryBuilder(queryStack, parent.getSchema()).buildQuery();

      searchMonitor.beginTrackingQuery(id,
                                       parentName,
                                       queryStackSize,
                                       searchQuery,
                                       includeKeys,
                                       includeValues,
                                       attributeSet,
                                       groupByAttributes,
                                       sortAttributes,
                                       aggPairs,
                                       resultLimit,
                                       batchLimit);

      // test wink
      SearchResultsDisruptor.invokePartitionedSearchGate1();

      final DocIdList docIds;

      boolean isGroupBy = !groupByAttributes.isEmpty();
      if (isGroupBy) resultLimit = -1;

      SearchResultSource source = null;

      final Map<String, List<Aggregator>> aggsPerAttr = createAggregators(aggPairs);

      final Set<String> aggregatorFields = new HashSet<String>(aggsPerAttr.keySet());
      final boolean includeCount = aggregatorFields.remove(COUNT_AGG_NAME);

      List<IndexQueryResult> firstBatch = new ArrayList<IndexQueryResult>();

      final Map<String, ValueType> requestedFields = new HashMap<String, ValueType>();
      final Set<String> reqFieldNames = new HashSet<String>();
      for (String attrKey : attributeSet) {
        reqFieldNames.add(attrKey);
      }

      for (String attrKey : groupByAttributes) {
        reqFieldNames.add(attrKey);
      }

      for (NVPair attrKey : sortAttributes) {
        reqFieldNames.add(attrKey.getName());
      }

      for (String aggFieldName : aggregatorFields) {
        reqFieldNames.add(aggFieldName);
      }

      for (String fieldName : reqFieldNames) {
        requestedFields.put(fieldName, getTypeForAttribute(fieldName));
      }

      if (includeKeys) reqFieldNames.add(LuceneIndex.KEY_FIELD_NAME);
      if (includeValues) reqFieldNames.add(LuceneIndex.VALUE_FIELD_NAME);

      // similar to DEV-7048: force aggregator attributes into result set, to correctly aggregate across all segments
      final Set<String> reqAttributes = new HashSet<String>(attributeSet);
      if (resultLimit > 0) {
        reqAttributes.addAll(aggregatorFields);
      }

      final QueryInputs params = new QueryInputs() {

        @Override
        public boolean includeValues() {
          return includeValues;
        }

        @Override
        public boolean includeKeys() {
          return includeKeys;
        }

        @Override
        public List<NVPair> getSortAttributes() {
          return sortAttributes;
        }

        @Override
        public Set<String> getGroupByAttributes() {
          return groupByAttributes;
        }

        @Override
        public Set<String> getFieldNamesToLoad() {
          return reqFieldNames;
        }

        @Override
        public Set<String> getAttributes() {
          return reqAttributes;
        }

        @Override
        public Map<String, ValueType> getFieldSchema() {
          return requestedFields;
        }
      };

      Map<Set<NVPair>, GroupedQueryResult> uniqueGroups = new HashMap<Set<NVPair>, GroupedQueryResult>();
      if (resultLimit == 0) {
        docIds = new EmptyDocIdList();
      } else {
        final SimpleCollector[] collectors = new SimpleCollector[readers.length];
        Collection<Callable<Void>> searchTasks = new ArrayList<Callable<Void>>(readers.length);
        for (int i = 0; i < readers.length; i++) {
          final IndexReader r = readers[i];

          final SimpleCollector c = new SimpleCollector(params, dataReader.getBase(i), resultLimit);
          collectors[i] = c;
          searchTasks.add(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              IndexSearcher searcher = new IndexSearcher(r);
              searcher.search(searchQuery, c);
              return null;
            }
          });
        }
        execSvc.invokeAll(searchTasks);

        final CompositeCollector merger = new CompositeCollector(dataReader, params, resultLimit);

        for (SimpleCollector collector : collectors) {
          merger.combine(collector.getDocIds());
        }
        merger.sort();
        merger.truncateIfNeeded();
        docIds = merger;
      }

      int resultCount = 0;

      for (int i = 0; i < docIds.size(); i++) {
        int docId = docIds.get(i);
        Document doc = dataReader.document(docId, reqFieldNames);

        final List<NVPair> attributes = new ArrayList<NVPair>(reqAttributes.size());
        Set<NVPair> groupByAttrs = isGroupBy ? new HashSet<NVPair>(groupByAttributes.size()) : Collections.EMPTY_SET;
        List<NVPair> sortAttributesList = sortAttributes.isEmpty() ? Collections.EMPTY_LIST
            : new ArrayList<NVPair>(sortAttributes.size());
        Collection<NVPair> aggregators = aggregatorFields.isEmpty() ? Collections.<NVPair> emptyList()
            : new ArrayList<NVPair>(aggregatorFields.size());

        loadDocumentData(doc, params, attributes, groupByAttrs, sortAttributesList);

        for (String attrKey : aggregatorFields) {
          ValueType type = requestedFields.get(attrKey);
          Object attrValue = getFieldValue(doc, attrKey, type);
          aggregators.add(AbstractNVPair.createNVPair(attrKey, attrValue, type));
        }

        IndexQueryResult result;
        if (isGroupBy) {
          result = new GroupedIndexQueryResultImpl(attributes, sortAttributesList, groupByAttrs,
                                                   new ArrayList<Aggregator>());
          putInGroup(result, aggregators, uniqueGroups, aggsPerAttr);
        } else {
          if (includeCount) {
            Collection<Aggregator> countAggs = aggsPerAttr.get(COUNT_AGG_NAME); // 1 element collection expected
            if (countAggs.isEmpty()) throw new AssertionError("Count aggregator: expected non-empty singleton list");
            Count count = (Count) countAggs.iterator().next();
            count.accept(docId);
          }

          for (NVPair attr : aggregators) {
            Collection<Aggregator> attrAggs = aggsPerAttr.get(attr.getName());
            if (attrAggs == null) throw new AssertionError();

            for (Aggregator agg : attrAggs) {
              try {
                // This reverse conversion to String is to prevent ClassCastException in
                // AbstractNVPair.createNVPair(String, Object, ValueType) when aggregator is serialized back inside
                // reply msg
                agg.accept(ValueType.ENUM == attr.getType() ? AbstractNVPair.enumStorageString((EnumNVPair) attr)
                    : attr.getObjectValue());
              } catch (IllegalArgumentException e) {
                throw new IndexException(e);
              }
            }
          }

          // Skip materializing result unless needed
          if (includeKeys || includeValues || !attributes.isEmpty()) {

            String key = includeKeys ? doc.get(LuceneIndex.KEY_FIELD_NAME) : null;
            ValueID value = includeValues ? (ValueID) getFieldValue(doc, LuceneIndex.VALUE_FIELD_NAME,
                                                                    ValueType.VALUE_ID) : ValueID.NULL_ID;
            result = new NonGroupedIndexQueryResultImpl(key, value, attributes, sortAttributesList);
            DeferredQueryResult toSave = new DeferredQueryResult((NonGroupedQueryResult) result, docId);
            if (firstBatch.size() == batchLimit) {
              // Create results source if needed
              if (source == null) {
                source = pagedResultSourceFactory.createSource(dataReader, params, docIds.size());
                // Catch up with first batch
                for (IndexQueryResult res : firstBatch) {
                  source.acceptResult((DeferredQueryResult) res);
                }
              }
              // optimization: depending on source type, we can throw the rest of doc ids in it
              // and skip loading results the rest of the way
              if (source instanceof CachedDocIdResultSource && aggPairs.isEmpty()) {
                for (int n = i; n < docIds.size(); n++) {
                  DeferredQueryResult res = new DeferredQueryResult(null, docIds.get(n));
                  resultCount++;
                  source.acceptResult(res);
                }
                break;
              }
              source.acceptResult(toSave);
            } else {
              firstBatch.add(toSave);
            }
            resultCount++;
          }
        }
      } // end of result collection loop

      SearchResult<IndexQueryResult> reply;
      if (isGroupBy) {
        List<IndexQueryResult> groups = new ArrayList<IndexQueryResult>(uniqueGroups.values());
        if (isSorted) {
          Collections.sort(groups, new QueryResultComparator(sortAttributes));
        }
        // Unlimited groups allowed here
        firstBatch = groups;
        resultCount = firstBatch.size();
        reply = new SearchResult(resultCount, firstBatch, Collections.EMPTY_LIST, docIds.size() > 0);
      } else {

        List<Aggregator> destAggs = new ArrayList<Aggregator>();
        for (Collection<Aggregator> attrAggs : aggsPerAttr.values())
          destAggs.addAll(attrAggs);
        reply = new SearchResult(resultCount, firstBatch, destAggs, docIds.size() > 0);
      }

      if (!aggPairs.isEmpty()) reorderAggregators(aggPairs, isGroupBy, reply);

      if (isGroupBy) {
        searchMonitor.finishTrackingQuery(id);
        resultSources.remove(id);
      } else {
        // Cache result source only if result set exceeds first batch size limit
        if (source == null) {
          searchMonitor.finishTrackingQuery(id);
          resultSources.remove(id);
        } else {
          source.setAggregatorValues(reply.getAggregators());
          SearchResultSource was = resultSources.put(id, source);
          if (was == null) {
            // someone closed it already, before we even reported results. bail hard.
            searchMonitor.finishTrackingQuery(id);
            resultSources.remove(id);
            source.close();
            source = null;
          }
        }
      }
      try {
        // We don't need the reader anymore unless it will be used for pagination
        if (source == null) dataReader.close();
      } catch (IOException e) {
        throw new IndexException(e);
      }
      if (reply != null) {
        searchMonitor.trackQueryResults(id, reply.getQueryResults().size());
      }
      return reply;

    } catch (Exception t) {
      try {
        searchMonitor.finishTrackingQuery(id);
      } catch (Throwable noop) {
      }
      SearchResultSource resSource = resultSources.remove(id);
      if (resSource != null && resSource != SearchResultSource.NULL_SOURCE) {
        try {
          resSource.close();
        } catch (IOException e) {
          // ignore here
        }
      }
      log.error("Query execution terminated abnormally", t);
      // Don't rewrap
      throw t instanceof IndexException ? (IndexException) t : new IndexException(t);
    }
  }

  private void putInGroup(IndexQueryResult res, Collection<NVPair> resultAggs,
                          Map<Set<NVPair>, GroupedQueryResult> uniqueGroups,
                          Map<String, List<Aggregator>> aggregators) {

    GroupedQueryResult group = (GroupedQueryResult) res;
    Set<NVPair> groupBy = group.getGroupedAttributes();
    fillInAggregators(group, resultAggs, aggregators);

    GroupedQueryResult dest = uniqueGroups.get(groupBy);
    if (dest == null) {
      uniqueGroups.put(groupBy, group);
    } else ResultTools.aggregate(dest.getAggregators(), group.getAggregators());

  }

  private void fillInAggregators(GroupedQueryResult result, Collection<NVPair> resultAggs,
                                 Map<String, List<Aggregator>> aggs) {
    List<Aggregator> dest = result.getAggregators();
    Collection<Aggregator> countAggs = aggs.get(COUNT_AGG_NAME); // 1 element collection
    if (countAggs != null) {
      if (countAggs.isEmpty()) throw new AssertionError("Count aggregator: expected non-empty singleton list");
      Count count = (Count) countAggs.iterator().next();
      Count clone = new Count(count.getAttributeName(), count.getType());
      clone.accept(result);
      dest.add(clone);
    }

    for (NVPair attr : resultAggs) {
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
    AttributeProperties attrProps = parent.getSchema().get(attributeName);
    ValueType type;
    if (attrProps != null) {
      type = attrProps.isEnum() ? ValueType.ENUM : Enum.valueOf(ValueType.class, attrProps.getType());
    } else type = null;

    // XXX: do error checking and optimization here when decoding the enum ordinal
    AggregatorOperations aggregatorType = AggregatorOperations.values()[enumPair.getOrdinal()];
    return AbstractAggregator.aggregator(aggregatorType, attributeName, type);
  }

  private ValueType getTypeForAttribute(String attrName) {
    AttributeProperties attrProps = parent.getSchema().get(attrName);
    return attrProps == null ? null : (attrProps.isEnum() ? ValueType.ENUM : Enum.valueOf(ValueType.class,
                                                                                          attrProps.getType()));
  }

  private void cleanupResults(ResultsFilter filter) throws IndexException {
    for (Map.Entry<QueryID, SearchResultSource> entry : resultSources.entrySet()) {
      QueryID id = entry.getKey();
      if (!filter.accept(id)) {
        SearchResultSource results = entry.getValue();
        try {
          if (results != SearchResultSource.NULL_SOURCE) {
            resultSources.remove(id);
            results.close();
            searchMonitor.finishTrackingQuery(id);
          }
        } catch (IOException e) {
          throw new IndexException(e);
        }
      }
    }

  }

  private interface DocIdList {
    int size();

    int get(int index);
  }
  
  private static class EmptyDocIdList implements DocIdList {

    @Override
    public int size() {
      return 0;
    }

    @Override
    public int get(int index) {
      throw new NoSuchElementException("index: " + index);
    }

 }

  private static class IntList {

    private int       size = 0;
    private Integer[] data;

    IntList() {
      this(16);
    }

    IntList(int cap) {
      data = new Integer[cap];
    }

    int size() {
      return size;
    }

    void add(int toAdd) {
      if (size == data.length) {
        Integer[] temp = new Integer[data.length * 2];
        System.arraycopy(data, 0, temp, 0, data.length);
        data = temp;
      }

      data[size++] = toAdd;

    }

    void addAll(IntList ids) {
      int room = data.length - size;
      if (ids.size() <= room) {
        System.arraycopy(ids.data, 0, data, size, ids.size());
      } else {
        Integer[] temp = new Integer[size + ids.size()];
        System.arraycopy(data, 0, temp, 0, size);
        System.arraycopy(ids.data, 0, temp, size, ids.size());
        data = temp;
      }
      size += ids.size();
    }

    Integer get(int index) {
      return data[index];
    }

    void truncate(int newSize) {
      if (size < newSize) throw new IllegalArgumentException();

      Integer[] dest = new Integer[newSize];
      System.arraycopy(data, 0, dest, 0, newSize);
      data = dest;
      size = newSize;
    }
  }

  private static class CompositeCollector implements DocIdList {
    private final MultiReader docReader;
    private final QueryInputs inputs;
    private final int         maxResults;
    private final IntList     ids = new IntList();

    private final class SortFieldSource implements SortFieldProvider {
      private final int         id;
      private final Set<String> sortFields;
      private List<NVPair>      sortCache;
    
      private SortFieldSource(int docId, Set<String> sortBy) {
        id = docId;
        sortFields = sortBy;
      }
    
      @Override
      public List<NVPair> getSortAttributes() {
        if (sortCache == null) {
          try {
            sortCache = loadSortFields(docReader.document(id, sortFields), inputs);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return sortCache;
      }
    
    }

    CompositeCollector(MultiReader reader, QueryInputs params, int maxResults) {
      this.docReader = reader;
      this.maxResults = maxResults;
      this.inputs = params;
    }

    @Override
    public int size() {
      return ids.size();
    }

    @Override
    public int get(int index) {
      return ids.get(index);
    }

    void combine(IntList src) {
      ids.addAll(src);
      
      src.data = null; // help GC
    }

    void truncateIfNeeded() {
      if (maxResults >= 0 && ids.size() > maxResults) {
        ids.truncate(maxResults);
      }

    }

    private void sort() {
      if (inputs.getSortAttributes().isEmpty() || !inputs.getGroupByAttributes().isEmpty()) return;
      final Comparator<SortFieldProvider> cmp = new QueryResultComparator(inputs.getSortAttributes());
      ids.truncate(ids.size());
      List<NVPair> sortBy = inputs.getSortAttributes();
      final Set<String>  sortFields = new HashSet<String>(sortBy.size());
        for (NVPair nv : sortBy) {
          sortFields.add(nv.getName());
        }

      final Map<Integer, SortFieldProvider> sortFieldCache = new HashMap<Integer, SortFieldProvider>(ids.size());
      Comparator docCmp = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          SortFieldProvider sf1 = sortFieldCache.get(o1);
          if (sf1 == null) {
            sf1 = new SortFieldSource(o1, sortFields);
            sortFieldCache.put(o1, sf1);
          }
          SortFieldProvider sf2 = sortFieldCache.get(o2);
          if (sf2 == null) {
            sf2 = new SortFieldSource(o2, sortFields);
            sortFieldCache.put(o2, sf2);
          }

          return cmp.compare(sf1, sf2);
        }
      };
      
      if (maxResults > 0 && ids.size() > maxResults * 2) {
        PriorityQueue<Integer> queue = new PriorityQueue<Integer>(maxResults, Collections.reverseOrder(docCmp));
        for (int i = 0; i < maxResults; i++) {
          queue.add(ids.get(i));
        }
        for (int i = maxResults; i < ids.size; i++) {
          Integer docId = ids.get(i);
          Integer toPurge; // keep sort value cache size constant
          if (docCmp.compare(docId, queue.peek()) < 0) {
            toPurge = queue.remove();
            queue.offer(docId);
          } else {
            toPurge = docId;
          }
          sortFieldCache.remove(toPurge);
        }
        for (int i = maxResults - 1; i >= 0; i--) {
          ids.data[i] = queue.remove();
        }
      } else {
        Arrays.sort(ids.data, docCmp);
      }
    }

  }

  private static class SimpleCollector extends Collector {

    private final int                            maxResults;
    private final boolean                        unbounded;
    /**
     * Base in implicit top level reader
     */
    private final int     topLevelBase;
    /**
     * "Relative" base for atomic readers delivered via {@link #setNextReader(AtomicReaderContext)}
     */
    private int           base;
    private final IntList ids = new IntList();
    
    private SimpleCollector(QueryInputs params, int parentBase, int maxResults) {
      List<NVPair> sortBy = params.getSortAttributes();
      this.unbounded = sortBy.size() > 0 || maxResults < 0;
      this.maxResults = maxResults;
      this.topLevelBase = parentBase;
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
    public void setNextReader(AtomicReaderContext reader) {
      base = topLevelBase + reader.docBase;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

    IntList getDocIds() {
      return ids;
    }

  }

}

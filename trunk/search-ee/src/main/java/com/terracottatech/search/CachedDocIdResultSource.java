/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import static com.terracottatech.search.LuceneIndex.getFieldValue;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import com.terracottatech.search.aggregator.Aggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class CachedDocIdResultSource implements SearchResultSource {
  private final IndexReader reader;
  private final IntList     docIds;
  private final QueryInputs queryArgs;
  private List<Aggregator>  aggregators;

  CachedDocIdResultSource(IndexReader srcReader, QueryInputs params, int totalSize) {
    reader = srcReader;
    queryArgs = params;
    docIds = new IntList(totalSize);
  }

  @Override
  public void acceptResult(DeferredQueryResult result) {
    docIds.add(result.getLuceneDocId());
  }

  @Override
  public SearchResult<NonGroupedQueryResult> getResults(int start, int size) throws IndexException {

    int end = Math.min(docIds.size, start + size); // one past the limit
    if (size <= 0 || start < 0) throw new IllegalArgumentException(String.format("start=%d, size=%d", start, size));
    List<NonGroupedQueryResult> resList = new ArrayList<NonGroupedQueryResult>(size);
    Set<NVPair> groupByAttrs = Collections.EMPTY_SET; // group by not supported
    try {
      // Guard against possible races with close(); lock scope intentionally wider than necessary to avoid entering mutex in every loop iteration
      synchronized (reader) {
        if (reader.getRefCount() == 0) throw new IndexException("Result set already closed.");
        for (int i = start; i < end; i++) {
          final List<NVPair> attributes = new ArrayList<NVPair>(queryArgs.getAttributes().size());
          List<NVPair> sortAttributesList = new ArrayList<NVPair>(queryArgs.getSortAttributes().size());
  
          Document doc = reader.document(docIds.get(i), queryArgs.getFieldNamesToLoad());
          SearchResultsManager.loadDocumentData(doc, queryArgs, attributes, groupByAttrs, sortAttributesList);
          String key = queryArgs.includeKeys() ? doc.get(LuceneIndex.KEY_FIELD_NAME) : null;
          ValueID value = queryArgs.includeValues() ? (ValueID) getFieldValue(doc, LuceneIndex.VALUE_FIELD_NAME,
                                                                              ValueType.VALUE_ID) : ValueID.NULL_ID;
          resList.add(new NonGroupedIndexQueryResultImpl(key, value, attributes, sortAttributesList));
  
        }
      }
      return new SearchResult<NonGroupedQueryResult>(docIds.size(), resList, aggregators,
                                                     docIds.size() > 0);
    } catch (IOException e) {
      throw new IndexException(e);
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (reader) {
      reader.close();
    }
  }

  private static class IntList {

    private int   size = 0;
    private final int[] data;

    IntList(int cap) {
      data = new int[cap];
    }

    int size() {
      return size;
    }

    void add(int toAdd) {
      data[size++] = toAdd;
    }

    int get(int index) {
      return data[index];
    }

  }

  @Override
  public void setAggregatorValues(List<Aggregator> aggregators) {
    this.aggregators = Collections.unmodifiableList(aggregators);
  }

}

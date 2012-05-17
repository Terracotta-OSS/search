/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.aggregator.Aggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ResultTools {
  public static void mergeGroupedResults(List<? extends IndexQueryResult> indexResults) {
    List<GroupedQueryResult> groups = (List<GroupedQueryResult>) indexResults;
    Map<Set<NVPair>, GroupedQueryResult> uniqueGroups = new LinkedHashMap<Set<NVPair>, GroupedQueryResult>();

    for (GroupedQueryResult group : groups) {
      Set<NVPair> groupBy = group.getGroupedAttributes();

      GroupedQueryResult dest = uniqueGroups.get(groupBy);
      if (dest == null) uniqueGroups.put(groupBy, group);
      else {
        aggregate(dest.getAggregators(), group.getAggregators());
      }
    }

    groups.clear();
    groups.addAll(uniqueGroups.values());
  }

  public static void aggregate(List<Aggregator> aggregates, List<Aggregator> incoming) {
    if (incoming.isEmpty()) {
      return;
    } else {
      if (aggregates.isEmpty()) {
        aggregates.addAll(incoming);
      } else {
        for (int i = 0; i < aggregates.size(); i++) {
          aggregates.get(i).accept(incoming.get(i));
        }
      }
    }
  }

  public static <T extends IndexQueryResult> List<T> nextIndexResults(Collection<List<T>> resultsFromAllIndexes,
                                                                      List<NVPair> sortBy) {
    final Comparator<IndexQueryResult> comp = new QueryResultComparator(sortBy);
    return Collections.min(resultsFromAllIndexes, new Comparator<List<T>>() {

      public int compare(List<T> o1, List<T> o2) {
        IndexQueryResult head1 = o1.isEmpty() ? null : o1.get(0);
        IndexQueryResult head2 = o2.isEmpty() ? null : o2.get(0);
        return head1 == null && head2 == null ? 0 : (head1 == null ? 1 : (head2 == null ? -1 : comp.compare(head1,
                                                                                                            head2)));
      }

    });
  }

  public static <T extends IndexQueryResult> List<T> mergeSort(Collection<List<T>> idxResults, List<NVPair> sortBy) {
    T lowest = null;
    List<T> sorted = new ArrayList<T>();
    do {
      List<T> next = nextIndexResults(idxResults, sortBy);
      if (next.isEmpty()) lowest = null;
      else {
        lowest = next.remove(0);
        sorted.add(lowest);
      }
    } while (lowest != null);
    return sorted;
  }

}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import com.terracottatech.search.AbstractNVPair.EnumNVPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class QueryResultComparator implements Comparator<IndexQueryResult> {

  private final Collection<Comparator<IndexQueryResult>> components = new ArrayList<Comparator<IndexQueryResult>>();

  public QueryResultComparator(Collection<? extends NVPair> sortBy) {
    for (EnumNVPair sortAttributePair : (Collection<EnumNVPair>) sortBy) {
      final String attributeName = sortAttributePair.getName();
      // Look up by ordinal b/c direct comparison not possible due to object value for enums loaded by calling class's
      // classloader, which is different from one for this class
      final boolean isDesc = SortOperations.DESCENDING.equals(SortOperations.values()[sortAttributePair.getOrdinal()]);

      components.add(new Comparator<IndexQueryResult>() {

        @Override
        public int compare(IndexQueryResult res1, IndexQueryResult res2) {
          List<NVPair> o1 = res1.getSortAttributes();
          List<NVPair> o2 = res2.getSortAttributes();

          if (o1.size() != o2.size()) throw new IllegalArgumentException(String
              .format("Non-equal sorting for query results: %s, %s", res1, res2));
          int n = 0;
          for (NVPair sortField1 : o1) {
            if (sortField1.getName().equals(attributeName)) {
              NVPair sortField2 = o2.get(n);
              // NOTE: not validating types due to NullNVPair having its own type, and we must be able to handle nulls
              // in sort fields
              if (!sortField1.getName().equals(sortField2.getName())) throw new IllegalArgumentException(String
                  .format("Query results contain incompatible sort fields: %s, %s", sortField1, sortField2));

              // Move nulls to the front, regardless of desired search order
              int comp = ValueType.NULL == sortField1.getType() || ValueType.NULL == sortField2.getType() ? 1
                  : (isDesc ? -1 : 1);
              return sortField1.compareTo(sortField2) * comp;
            }
            n++;
          }
          throw new IllegalArgumentException(String
              .format("Unable to locate sort attribute %s in result sort fields %s", attributeName, o1));
        }

      });
    }
  }

  @Override
  public int compare(IndexQueryResult res1, IndexQueryResult res2) {
    for (Comparator<IndexQueryResult> comp : components) {
      int res = comp.compare(res1, res2);
      if (res != 0) return res;
    }
    return 0;
  }

}

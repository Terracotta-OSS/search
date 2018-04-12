/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

/**
 * This class is here to provide gates/spys for the SearchResultsManager
 * execution paths. In this case, 1 callback is available, in the
 * gap between the entry into the execute() method and the actual
 * lucene index execution. This was done to allow for testing a race
 * condition case.
 */
public class SearchResultsDisruptor {

  private static volatile Runnable partitionedSearchGate1 = null;

  public static void invokePartitionedSearchGate1() {
    Runnable gate = partitionedSearchGate1;
    if (gate != null) {
      gate.run();
    }
  }

  public static void setPartitionedSearchGate(Runnable r) {
    partitionedSearchGate1 = r;
  }
}

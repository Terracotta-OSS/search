/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.concurrent.atomic.AtomicLong;

class TestSearchBuilder extends SearchBuilder {
  private static final AtomicLong queryIdGen = new AtomicLong();

  TestSearchBuilder() {
    queryId(queryIdGen.getAndIncrement());
  }
}

/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.util.List;
import java.util.Map;

public interface SyncSnapshot {
  Map<String, List<IndexFile>> getFilesToSync();

  void release();
}

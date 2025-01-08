/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search;

import java.util.List;
import java.util.Map;

public interface SyncSnapshot {
  Map<String, List<IndexFile>> getFilesToSync();

  void release();
}

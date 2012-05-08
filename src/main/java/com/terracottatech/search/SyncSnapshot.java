/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.List;
import java.util.Map;

public interface SyncSnapshot {
  Map<String, List<IndexFile>> getFilesToSync();

  void release();
}

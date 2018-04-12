/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.util.List;
import java.util.Map;
import java.util.Set;

interface QueryInputs {

  boolean includeKeys();

  boolean includeValues();

  Map<String, ValueType> getFieldSchema();

  Set<String> getFieldNamesToLoad();

  Set<String> getAttributes();

  Set<String> getGroupByAttributes();

  List<NVPair> getSortAttributes();
}

/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search;

public interface LoggerFactory {

  Logger getLogger(String name);

  Logger getLogger(Class c);

}

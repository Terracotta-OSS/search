/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

public interface LoggerFactory {

  Logger getLogger(String name);

  Logger getLogger(Class c);

}

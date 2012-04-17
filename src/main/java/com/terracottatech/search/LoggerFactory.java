/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

public interface LoggerFactory {

  Logger getLogger(String name);

  Logger getLogger(Class c);

}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

public interface Logger {
  void debug(Object message);

  void debug(Object message, Throwable t);

  void error(Object message);

  void error(Object message, Throwable t);

  void fatal(Object message);

  void fatal(Object message, Throwable t);

  void info(Object message);

  void info(Object message, Throwable t);

  void warn(Object message);

  void warn(Object message, Throwable t);

  boolean isDebugEnabled();

  boolean isInfoEnabled();

  String getName();

}

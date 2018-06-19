/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package com.terracottatech.search;

import org.apache.lucene.util.InfoStream;

import java.io.IOException;

public class TCInfoStream extends InfoStream {

  private final Logger logger;

  public TCInfoStream(Logger logger) {
    this.logger = logger;
  }

  @Override
  public void message(String component, String message) {
    logger.debug(component + " " + component + " [" + Thread.currentThread().getName() + "]: " + message);
  }

  @Override
  public boolean isEnabled(String component) {
    if (component.equals("IFD")) {
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
  }
}

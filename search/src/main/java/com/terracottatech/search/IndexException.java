/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

public class IndexException extends Exception {

  public IndexException() {
    super();
  }

  public IndexException(Throwable cause) {
    super(cause);
  }

  public IndexException(String message) {
    super(message);
  }

  public IndexException(String message, Throwable cause) {
    super(message, cause);
  }

}

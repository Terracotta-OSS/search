/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

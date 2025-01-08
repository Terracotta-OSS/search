/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search;

public enum SearchMetaData {
  KEY("KEY@"), COMMAND("COMMAND@"), ATTR("ATTR@"), VALUE("VALUE@"), PREV_VALUE("PREV_VALUE@"), CACHENAME("CACHENAME@"), CLIENT_ID(
      "CLIENT_ID@"), REQUEST_ID("REQUEST_ID@");

  private final String tag;

  SearchMetaData(String text) {
    tag = text;
  }

  @Override
  public String toString() {
    return tag;
  }

  public boolean equals(String s) {
    return toString().equals(s);
  }
}

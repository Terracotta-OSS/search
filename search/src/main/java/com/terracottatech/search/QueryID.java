/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public final class QueryID implements Serializable {
  public final long requesterId;
  public final long queryId;
  private transient int hash;

  public QueryID(long requesterId, long queryId) {
    this.requesterId = requesterId;
    this.queryId = queryId;
    hash = computeHash();
  }

  private int computeHash() {
    final int prime = 31;
    int result = prime + (int) (requesterId ^ (requesterId >>> 32));
    result = prime * result + (int) (queryId ^ (queryId >>> 32));

    return result;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    hash = computeHash();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof QueryID)) return false;
    QueryID q2 = (QueryID) obj;
    return requesterId == q2.requesterId && queryId == q2.queryId;
  }

  @Override
  public String toString() {
    return new StringBuilder(128).append("requesterId=").append(requesterId).append(", queryId=").append(queryId)
        .toString();
  }

}
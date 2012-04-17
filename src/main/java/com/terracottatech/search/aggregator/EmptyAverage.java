/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search.aggregator;


public class EmptyAverage extends Average {

  private Average delegate;

  public EmptyAverage(String attributeName) {
    super(attributeName, null);
  }

  public void accept(Object input) throws IllegalArgumentException {
    if (delegate != null) {
      delegate.accept(input);
    } else if (input != null) { throw new IllegalArgumentException(
                                                                   "Unexpected visitor on EmptyAverage aggregator [attribute:"
                                                                       + getAttributeName() + " value:" + input + "]"); }
  }

  public void accept(Aggregator incoming) throws IllegalArgumentException {
    if (incoming instanceof Average) {
      if (delegate == null) {
        delegate = (Average) incoming;
      } else {
        delegate.accept(incoming);
      }
    } else {
      throw new IllegalArgumentException();
    }
  }

  public Object getResult() {
    if (delegate == null) {
      return null;
    } else {
      return delegate.getResult();
    }
  }

}

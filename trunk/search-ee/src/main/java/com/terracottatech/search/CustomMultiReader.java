/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;

/**
 * Irritating extension of {@link MultiReader}, just to open up {@link MultiReader#readerBase(int)} to the world.
 */
public class CustomMultiReader extends MultiReader {

  public CustomMultiReader(IndexReader[] subReaders) {
    super(subReaders, false);
  }

  public int getBase(int readerIdx) {
    return readerBase(readerIdx);
  }
}

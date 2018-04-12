/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class IdxCloseFailTest extends SearchTestBase {

  @Override
  protected Configuration getConfig() {
    return new Configuration(4, 10, false, false, false, 0, 0, 0);
  }
  
  @Test
  public void failOnShutdown() throws IndexException, IOException {
    if (File.separatorChar != '\\') {
      // Windows doesn't do this.
      File luceneDir = getLuceneDir();

      addData(8);

      assertEquals(1, idxManager.getSearchIndexNames().length);
      assertEquals(getName(), idxManager.getSearchIndexNames()[0]);

      File cacheDir = luceneDir.listFiles()[0];
      assertTrue(cacheDir.getPath(), cacheDir.isDirectory());
      makeReadOnly(cacheDir);

      try {
        idxManager.shutdown();
        fail("Expected exception not thrown");
      } catch (IndexException e) {
        System.out.println("Caught expected exception: " + e);
      }

      makeWritable(cacheDir);
    }
  }

  @Test
  public void failOnIdxDestroy() throws Exception {
    if(File.separatorChar!='\\') {
      // Windows doesn't do this.

      addData(8);

      File cacheDir = getLuceneDir().listFiles()[0];
      assertTrue(cacheDir.getPath(), cacheDir.isDirectory());
      makeReadOnly(cacheDir);

      try {
        idxManager.destroy(getName(), new NullProcessingContext());
        fail("Expected exception not thrown");
      } catch (IndexException e) {
        System.out.println("Caught expected exception: " + e);
      }

      makeWritable(cacheDir);
    }
  }
  
  private void makeReadOnly(File cacheDir) {
    for (File idx : cacheDir.listFiles()) {
      if (idx.isDirectory()) {
        boolean cond = idx.setReadOnly();
        assertTrue(cond);
      }
    }
    
  }
  
  private void makeWritable(File cacheDir) {
    for (File idx : cacheDir.listFiles()) {
      if (idx.isDirectory()) assertTrue(idx.setWritable(true));
    }
    
  }
  
}

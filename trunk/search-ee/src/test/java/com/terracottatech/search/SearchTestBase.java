/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.terracottatech.search.SearchBuilder.Search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchTestBase extends Assert {

  @Rule
  public final TestName             testName      = new TestName();

  protected static final List<NVPair> EMPTY         = Collections.emptyList();
  private static final int          idxCt         = 4;

  protected LuceneIndexManager        idxManager;

  protected final Configuration       cfg = getConfig();
  protected final LoggerFactory         loggerFactory = new SysOutLoggerFactory();

  @After
  public void tearDown() throws Exception {
    if (idxManager != null) idxManager.shutdown();
    try {
      Util.deleteDirectory(getLuceneDir());
    } catch(Throwable e) {
    }
  }

  protected Configuration getConfig() {
    return new Configuration(idxCt, 10, false, false, true, 0, 0, 0);
  }
  
  @Before
  public void setUp() throws Exception {
    idxManager = newIndexManager(getLuceneDir(true));
  }

  protected LuceneIndexManager newIndexManager(File dir) {
    cfg.setDoAccessChecks(false);
    cfg.setMaxResultBatchSize(Search.BATCH_SIZE_UNLIMITED);
    return new LuceneIndexManager(dir, true, loggerFactory, cfg);
  }

  protected File getLuceneDir() throws IOException {
    return getLuceneDir(false);
  }

  private File getLuceneDir(boolean clean) throws IOException {
    File dir = new File(getTempDirectory(), getName());
    if (clean) {
      Util.cleanDirectory(dir);
    }
    return dir;
  }

  protected String getTempDirectory() {
    File testTempDir = new File("target/temp", getClass().getSimpleName());
    testTempDir.mkdirs();
    return testTempDir.getAbsolutePath();
  }

  public String getName() {
    return testName.getMethodName();
  }

  protected void addData(int size) throws IndexException {
    assertEquals(0, idxManager.getSearchIndexNames().length);

    List<NVPair> storeOnlyattributes = new ArrayList<NVPair>();
    storeOnlyattributes.add(new AbstractNVPair.ByteArrayNVPair("attr2", new byte[] { 6, 6, 6 }));

    for (int i = 0; i < size; i++) {
      // reverse the order, just for fun
      ValueID valueOid = new ValueID(size - 1 - i);
      idxManager.insert(getName(), "key-" + i, valueOid,
                        Collections.singletonList(AbstractNVPair.createNVPair("attr1", i + "-" + valueOid.toLong())),
                        storeOnlyattributes, i,
                        new NullProcessingContext());
    }

  }

}

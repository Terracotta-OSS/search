/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IdxCloseNoTxnsRemainTest extends SearchTestBase {

 
  /**
   * Verify that all txns are acknowledged after shutdown()/close() returns
   */
  @Test
  public void writeRaceWithShutdown() throws Exception {
    assertEquals(0, idxManager.getSearchIndexNames().length);

    List<NVPair> storeOnlyattributes = new ArrayList<NVPair>();
    storeOnlyattributes.add(new AbstractNVPair.ByteArrayNVPair("attr2", new byte[] { 6, 6, 6 }));
    int initSize = 10, nextSize = 5;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch counter = new CountDownLatch(initSize + nextSize);
    final AtomicInteger seq = new AtomicInteger(0);
    {
      for (int i = 0; i < initSize; i++) {
        final int n = i;
        ValueID valueOid = new ValueID(i);
        idxManager.insert(getName(), "key-" + i, valueOid,
                          Collections.singletonList(AbstractNVPair.createNVPair("attr1", i + "-" + valueOid.toLong())),
                          storeOnlyattributes, 1024,
                          new ProcessingContext() {
  
                            @Override
                            public void processed() {
                              try {
                                Thread.sleep(500);
                                startLatch.countDown();
                                counter.countDown();
                                assertEquals(seq.getAndIncrement(), n);
                              }
                              catch (InterruptedException e) {
                                e.printStackTrace();
                              }
                            }
          
          });
        }
      }
      if (!startLatch.await(30, TimeUnit.SECONDS)) fail("No acks received from initial writes");
      {
        for (int i = initSize; i < initSize + nextSize; i++) {
          final int n = i;

          ValueID valueOid = new ValueID(i);
          idxManager.insert(getName(), "key-" + i, valueOid,
                            Collections.singletonList(AbstractNVPair.createNVPair("attr1", i + "-" + valueOid.toLong())),
                            storeOnlyattributes, 1024,
                            new ProcessingContext() {
  
                              @Override
                              public void processed() {
                                counter.countDown();
                                assertEquals(seq.getAndIncrement(), n);
                              }
            
          });
        }
      }
      idxManager.shutdown();
      // This is a little racy in that shutdown() may not be synchronous wrt to acks from other ops in general. But
      // the timing of this particular test is set up such that it will be.
      assertEquals(0, counter.getCount());
  }

}

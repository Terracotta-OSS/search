/*
 * Copyright (c) 2011-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
 */
package com.terracottatech.search;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExplicitBlockingDeletePolicy extends IndexDeletionPolicy {
  private final CounterLock clock;
  private final IndexDeletionPolicy delegate;
  private LinkedBlockingQueue<IndexCommit> q = new LinkedBlockingQueue<IndexCommit>();

  public ExplicitBlockingDeletePolicy(IndexDeletionPolicy delegate) {
    this.clock = new CounterLock();
    this.delegate = delegate;
  }

  public ExplicitBlockingDeletePolicy(CounterLock clock, IndexDeletionPolicy delegate) {
    this.clock = clock;
    this.delegate = delegate;
  }

  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    if (!commits.isEmpty()) {
      if (clock.sharedLock()) {
        try {
          for (IndexCommit ic = q.poll(); ic != null; ic = q.poll()) {
            ic.delete();
          }
          delegate.onCommit(commits);
        } finally {
          clock.sharedUnlock();
        }
      } else {
        for (IndexCommit ic : commits) {
          q.add(ic);
        }
      }
    }
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    onCommit(commits);
  }

  public void pinDeletions() {
    clock.lockExclusively();
  }

  public void unpinDeletions() {
    clock.unlockExclusively();
  }

  @Override
  public IndexDeletionPolicy clone() {
    return new ExplicitBlockingDeletePolicy(clock, delegate.clone());
  }

  static class CounterLock {
    private final ReentrantReadWriteLock rwlock;
    private volatile int counter = 0;

    CounterLock() {
      this.rwlock = new ReentrantReadWriteLock(true);
    }

    void lockExclusively() {
      rwlock.writeLock().lock();
      try {
        counter++;
      } finally {
        rwlock.writeLock().unlock();
      }
    }

    void unlockExclusively() {
      rwlock.writeLock().lock();
      try {
        counter--;
        if (counter < 0) {
          throw new AssertionError();
        }
      } finally {
        rwlock.writeLock().unlock();
      }
    }

    public boolean sharedLock() {
      if (rwlock.readLock().tryLock()) {
        if (counter == 0) {
          // return with the read lock intact
          return true;
        }
        // unlock, no joy.
        rwlock.readLock().unlock();
      }
      return false;
    }

    public void sharedUnlock() {
      rwlock.readLock().unlock();
    }
  }
}

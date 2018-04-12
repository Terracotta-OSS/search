package com.terracottatech.search.store;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements {@link LockFactory} for a single in-process instance, meaning all locking will take place through this one
 * instance.This one differs from {@link SingleInstanceLockFactory} in that this is a little more concurrent while
 * acquiring and releasing locks. Only use this {@link LockFactory} when you are certain all IndexReaders and
 * IndexWriters for a given index are running against a single shared in-process Directory instance. This is currently
 * the default locking for OffHeapDirectory.
 * 
 * @see LockFactory
 */

public class ConcurrentLockFactory extends LockFactory {

  private final ConcurrentHashMap<String, String> locks = new ConcurrentHashMap<String, String>();

  @Override
  public Lock makeLock(String lockName) {
    return new LockObject(locks, lockName);
  }

  @Override
  public void clearLock(String lockName) {
    locks.remove(lockName);
  }

  private static final class LockObject extends Lock {

    String                                          lockName;
    private final ConcurrentHashMap<String, String> locks;

    public LockObject(ConcurrentHashMap<String, String> locks2, String lockName) {
      this.locks = locks2;
      this.lockName = lockName;
    }

    @Override
    public boolean obtain() {
      return locks.putIfAbsent(lockName, lockName) == null;
    }

    @Override
    public void release() {
      locks.remove(lockName);
    }

    @Override
    public boolean isLocked() {
      return locks.containsKey(lockName);
    }

    @Override
    public String toString() {
      return super.toString() + ": " + lockName;
    }
  }
}

/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;




public class UtilsTest {

  @After
  public void cleanup() throws Exception {
    Util.deleteDirectory(tmpDir);
  }

  @Before
  public void setup() throws Exception {
    boolean status = tmpDir.mkdirs();
    if(!status) {
      throw new IllegalStateException("Unable to create tmpdir " + tmpDir.getAbsolutePath());
    }
  }

  final File tmpDir = new File("target/temp", getClass().getSimpleName());

  @Test
  public void testShouldUpgrade() throws Exception {
    String cacheFileName = "shouldupgrade";
    File f = new File(tmpDir, cacheFileName);
    f.mkdirs();
    assertTrue("Should upgrade should return true", Util.shouldUpgrade(f));

    File hashFile = new File(f, SearchConsts.TERRACOTTA_HASH_FILE);
    hashFile.createNewFile();
    assertTrue("Should upgrade should return true", Util.shouldUpgrade(f));

    Util.storeContent(hashFile, "abc");
    assertTrue("Should upgrade should return true", Util.shouldUpgrade(f));

    String hashStr = Util.sanitizeCacheName(cacheFileName);

    f = new File(tmpDir, hashStr);
    f.mkdirs();
    hashFile = new File(f, SearchConsts.TERRACOTTA_HASH_FILE);
    Util.storeContent(hashFile, cacheFileName);
    assertFalse("Should upgrade should return false", Util.shouldUpgrade((File) f));
  }

  @Test
  public void testCollisions() throws Exception {
    String cacheName = "abc";
    String hashedNamed = Util.sanitizeCacheName(cacheName);
    File root = tmpDir;
    File cacheFile = new File(root, hashedNamed);
    Util.ensureDirectory(cacheFile);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_HASH_FILE), cacheName);

    assertFalse(Util.checkForCollision(root, hashedNamed, cacheName));
    //Alter the name of the cache file and use the hash of abc, this is to simulate collision
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName + 1);
    assertTrue(Util.checkForCollision(root, hashedNamed, cacheName));
  }

  @Test
  public void testUpgrade() throws Exception {
    String cacheName = "abc";
    File root = tmpDir;
    File cacheFile = new File(root, cacheName);
    Util.ensureDirectory(cacheFile);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName);

    assertTrue(Util.shouldUpgrade(cacheFile));
    String hashedNamed = Util.sanitizeCacheName(cacheName);

    Util.upgradeCache(root, cacheFile, cacheName , hashedNamed);
    File newCacheDir = new File(root, hashedNamed);
    assertTrue(newCacheDir.exists());
    assertFalse(cacheFile.exists());

    //Should upgrade on already upgraded cache directory should be false
    assertFalse(Util.shouldUpgrade(newCacheDir));
    Util.ensureDirectory(cacheFile);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName);

    assertTrue(Util.shouldUpgrade(cacheFile));
    try {
      Util.upgradeCache(root, cacheFile, cacheName , hashedNamed);
      fail("Illegal State exception should have been thrown when rename operations fail");
    } catch (IllegalStateException e) {
      //
    }

    Util.deleteDirectory(newCacheDir);
    Util.upgradeCache(root, cacheFile, cacheName , hashedNamed);
    assertTrue(newCacheDir.exists());
    assertFalse(cacheFile.exists());


  }

  @Test
  public void testCollisionWhileIndexUpgradation() throws Exception {
    String cacheName = "abc";
    File root = tmpDir;
    File cacheFile = new File(root, cacheName);
    Util.ensureDirectory(cacheFile);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName);

    assertTrue(Util.shouldUpgrade(cacheFile));
    String hashedNamed = Util.sanitizeCacheName(cacheName);
    Util.upgradeCache(root, cacheFile, cacheName, hashedNamed);

    //change the cache name to simulate collision
    File newCacheDir = new File(root, hashedNamed);
    Util.storeContent(new File(newCacheDir, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), "xyz");

    cacheFile = new File(root, cacheName);
    Util.ensureDirectory(cacheFile);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName);
    assertTrue(Util.shouldUpgrade(cacheFile));

    String hashNamed1 = Util.sanitizeCacheName(cacheName+0);
    Util.getOrCreateCacheGroupDir(root, cacheName,  true);
    //We got a collision and we adjusted the name and created a proper index directory
    assertTrue(new File(root, hashNamed1).exists());
  }

  @Test
  public void testCollisionWhileIndexCreation() throws Exception {
    String cacheName = "abc";
    Util.getOrCreateCacheGroupDir(tmpDir, cacheName,  false);
    String hashName = Util.sanitizeCacheName(cacheName);
    //cache index dir is created.
    File cacheDir = new File(tmpDir, hashName);
    assertTrue(cacheDir.exists());
    //Change the name of the cache file so that next time we try to create an index abc we get collision, this one
    //gets to be read as "xyz" instead of "abc" as we always take terracotta cache name file as source of truth
    Util.storeContent(new File(cacheDir, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), "xyz");
    Util.getOrCreateCacheGroupDir(tmpDir, cacheName,  false);
    String newCacheName = cacheName+0;
    assertTrue(new File(tmpDir, Util.sanitizeCacheName(newCacheName)).exists());
  }

  @Test
  public void testLogicalCollision() throws Exception {
    String cacheName = "foo";
    Util.getOrCreateCacheGroupDir(tmpDir, cacheName, false);
    String hashName = Util.sanitizeCacheName(cacheName);
    //cache index dir is created.
    File cacheDir = new File(tmpDir, hashName);
    assertTrue(cacheDir.exists());
    Util.storeContent(new File(cacheDir, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), "bar");

    cacheName = "foo0";
    Util.getOrCreateCacheGroupDir(tmpDir, cacheName, false);
    hashName = Util.sanitizeCacheName(cacheName);
    cacheDir = new File(tmpDir, hashName);
    assertTrue(cacheDir.exists());
    Util.storeContent(new File(cacheDir, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), "foo0");

    cacheName = "foo";
    Util.getOrCreateCacheGroupDir(tmpDir, cacheName, false);
    hashName = Util.sanitizeCacheName(cacheName+"01");
    //cache index dir is created.
    cacheDir = new File(tmpDir, hashName);
    assertTrue(cacheDir.exists());

  }


  @Test
  public void testStoreAndReadContent() throws Exception {
    String cacheName = "abc";
    File root = tmpDir;
    File cacheFile = new File(root, cacheName);
    Util.ensureDirectory(cacheFile);
    Util.storeContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE), cacheName);
    String content = Util.loadContent(new File(cacheFile, SearchConsts.TERRACOTTA_CACHE_NAME_FILE));
    assertEquals(cacheName, content);
  }


  @Test
  public void testFailoverToEscapeString() throws Exception {
    class MockMessageDigestProvider extends Util.MessageDigestProvider {
      @Override
      MessageDigest getMessageDigest() throws NoSuchAlgorithmException {
        throw new NoSuchAlgorithmException();
      }
    }

    MockMessageDigestProvider provider = new MockMessageDigestProvider();
    String name = "abc";
    String str = Util.sanitizeCacheName(name, provider);

    String output = Util.escapeString(name);

    assertEquals(str, output);


  }

}

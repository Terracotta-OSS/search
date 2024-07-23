/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import static com.terracottatech.search.SearchConsts.TERRACOTTA_CACHE_NAME_FILE;
import static com.terracottatech.search.SearchConsts.TERRACOTTA_HASH_FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

public class Util {

  private static final int            DEL      = 0x7F;
  private static final char           ESCAPE   = '%';
  private static final Set<Character> ILLEGALS = new HashSet<Character>();

  static class MessageDigestProvider {

    MessageDigest getMessageDigest() throws NoSuchAlgorithmException {
      return MessageDigest.getInstance("MD5");
    }

  }

  private static final MessageDigestProvider provider;

  static {
    ILLEGALS.add('/');
    ILLEGALS.add('\\');
    ILLEGALS.add('<');
    ILLEGALS.add('>');
    ILLEGALS.add(':');
    ILLEGALS.add('"');
    ILLEGALS.add('|');
    ILLEGALS.add('?');
    ILLEGALS.add('*');
    ILLEGALS.add('.');
    provider = new MessageDigestProvider();
  }




  static boolean shouldUpgrade(File rootDir, String cacheName) throws IOException {
    File cacheDir = new File(rootDir, escapeString(cacheName));
    return shouldUpgrade(cacheDir);
  }
  /**
   * Check if a given cache dir should be upgraded or not, depends on two condition, presence of the hash file, and
   * if the dir name is equal to hashed content stored in the terracotta hash file.
   *
   * @param cacheDir dir to be upgraded
   * @return true if to be upgraded
   * @throws Exception in case of any errors
   */

  static boolean shouldUpgrade(File cacheDir) throws IOException {
    if(!cacheDir.exists()) {
      return false;
    }
    File hashFile =  new File(cacheDir, TERRACOTTA_HASH_FILE);
    if (!hashFile.exists()) {
      return true;
    }
    String hash = sanitizeCacheName(loadContent(hashFile));
    return !hash.equals(cacheDir.getName());
  }

  static String loadContent(File f) throws IOException {
    FileInputStream in = null;
    try {
      in = new FileInputStream(f);

      StringBuilder sb = new StringBuilder();
      int read;
      byte[] buf = new byte[2];
      while ((read = in.read(buf)) != -1) {
        if (read != 2) { throw new IOException("read " + read + " bytes"); }
        char c = (char) ((buf[0] << 8) | (buf[1] & 0xff));
        sb.append(c);
      }
      return sb.toString();
    }  finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException ioe) {
          //
        }
      }
    }
  }

  static void storeContent(File f, String content) throws IOException {
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(f);
      byte[] byteRepr = getBytes(content);
      for (byte b : byteRepr) {
        out.write(b);
      }
      out.flush();
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException ioe) {
          //
        }
      }
    }

  }

  static void upgradeCache(File rootDir, String cacheName,
                           String adjustedString, String hashString) throws IOException {
    File cacheDir = new File(rootDir, escapeString(cacheName));
    upgradeCache(rootDir,cacheDir, adjustedString, hashString);
  }



  static void upgradeCache(File rootDir, File cacheDir,
                           String adjustedName, String hashString) throws IOException {
    storeContent(new File(cacheDir, TERRACOTTA_HASH_FILE), adjustedName);
    boolean status = cacheDir.renameTo(new File(rootDir, hashString));

    if(!status) {
      throw new IllegalStateException("Upgrading cache failed at rename stage");
    }

  }

  private static String getCollisionFreeCacheName(File rootDir, String cacheName) throws IOException{
    String hashString = sanitizeCacheName(cacheName);
    boolean collisionOccurred = checkForCollision(rootDir, hashString, cacheName);
    String adjustedName = cacheName;
    int i = 0;
    while (collisionOccurred) {
      adjustedName +=i++;
      hashString = sanitizeCacheName(adjustedName);
      collisionOccurred = checkForCollision(rootDir, hashString, cacheName);
    }
    return adjustedName;
  }

  /**
   * Check if an collision has occurred. The method works as following.
   *
   * Check if root dir + hash string exists, if no return false, if yes. Then check if cache name (i.e. adjusted name) is
   * same as the one stored in the terracotta cache name file, if same then no collision has occured, if different then
   * collision has occured.
   *
   * @param rootDir top level directory where all cache indexes are created
   * @param hashString hashed string of cache
   * @param originalName name of the cache
   * @return
   * @throws IOException
   */
  static boolean checkForCollision(File rootDir, String hashString, String originalName) throws IOException {
    File cacheDir = new File(rootDir, hashString);
    if(!cacheDir.exists()) {
      return false;
    }
    String name = loadCacheName(cacheDir);
    return !(originalName.equals(name));
  }


  private static String loadCacheName(File cacheDir) throws IOException{
    return loadContent(new File(cacheDir, TERRACOTTA_CACHE_NAME_FILE));
  }

  /**
   * Get or Create cache group directory based on lucene index directory
   *
   * @param rootDir root directory for lucene server
   * @param cacheName name of cache which group directory is being created
   * @param existing true if it is an existing index
   * @return
   * @throws IOException
   */
  static File getOrCreateCacheGroupDir(File rootDir, String cacheName,
                               boolean existing) throws IOException {
    String adjustedName = getCollisionFreeCacheName(rootDir, cacheName);
    String hashString = Util.sanitizeCacheName(adjustedName);
    File cacheDir = new File(rootDir, hashString);
    if(existing) {
      boolean shouldUpgrade = Util.shouldUpgrade(rootDir, cacheName);
      if(shouldUpgrade) {
        //Upgrade
        Util.upgradeCache(rootDir, cacheName, adjustedName, hashString);
      }
    } else {
      //write out hashfile in the cache directory which is newly created
      Util.ensureDirectory(cacheDir);
      storeContent(new File(cacheDir, TERRACOTTA_HASH_FILE), adjustedName);
    }
    return cacheDir;
  }
  /**
   * Convert the given string into a unique and legal path for all file systems
   */

  public static String sanitizeCacheName(String name) {
    return sanitizeCacheName(name, provider);
  }

  static String sanitizeCacheName(String name, MessageDigestProvider provider) {
    return hashCacheName(name, provider);
  }


  static String escapeString(String name) {
    int len = name.length();
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      char c = name.charAt(i);
      if (c <= ' ' || c >= DEL || (c >= 'A' && c <= 'Z') || ILLEGALS.contains(c) || c == ESCAPE) {
        sb.append(ESCAPE);
        sb.append(String.format("%04x", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static String hashCacheName(String name, MessageDigestProvider provider) {
    MessageDigest md;
    try {
      md = provider.getMessageDigest();
    } catch (NoSuchAlgorithmException e) {
      return escapeString(name);
    }
    byte[] hash;
    hash = md.digest(getBytes(name));
    StringBuilder sb = new StringBuilder(hash.length * 2);
    for(byte b : hash) {
      sb.append(String.format("%02x", b & 0xff ));
    }
    return sb.toString();
  }


  static byte[] getBytes(String name) {
    byte[] byteArray = new byte[name.length() * 2];
    int i = 0;
    for(char c : name.toCharArray()) {
      byteArray[i++] = (byte) (0xff & (c >> 8));
      byteArray[i++] = (byte) (0xff & c);
    }
    return byteArray;
  }

  public static synchronized void ensureDirectory(File dir) throws IOException {
    if (dir.exists() && !dir.isDirectory()) {
      //
      throw new IOException("Non-directory path exists at " + dir.getAbsolutePath());
    }

    if (dir.isDirectory()) { return; }

    boolean created = dir.mkdirs();
    if (!created) { throw new IOException("Cannot create directory: " + dir.getAbsolutePath()); }

    if (!dir.canWrite()) {
      //
      throw new IOException("Cannot write to [newly created] directory: " + dir.getAbsolutePath());
    }
  }

  public static void deleteDirectory(File dir) throws IOException {
    cleanDirectory(dir, true);
  }

  public static void cleanDirectory(File dir) throws IOException {
    cleanDirectory(dir, false);
  }

  private static void cleanDirectory(File dir, boolean deleteTop) throws IOException {
    if (!dir.exists()) { return; }

    if (dir.isFile()) { throw new IOException(dir + " is not a directory"); }

    for (File entry : dir.listFiles()) {
      if (entry.isDirectory()) {
        deleteDirectory(entry);
      } else {
        boolean deleted = entry.delete();
        if (!deleted) { throw new IOException("Cannot delete " + entry); }
      }
    }

    if (deleteTop) {
      boolean deleted = dir.delete();
      if (!deleted) { throw new IOException("Cannot delete " + dir); }
    }
  }

  public static void copyFile(File src, File dest) throws IOException {
    if (!src.isFile()) { throw new IOException(src + " is not a file"); }

    byte[] buf = new byte[8192];

    FileInputStream in = null;
    FileOutputStream out = null;

    try {
      in = new FileInputStream(src);
      out = new FileOutputStream(dest, false);

      int read;
      while ((read = in.read(buf)) != -1) {
        out.write(buf, 0, read);
      }
    } finally {
      closeQuietly(in);
      closeQuietly(out);
    }
  }

  public static void closeQuietly(InputStream in) {
    if (in != null) {
      try {
        in.close();
      } catch (IOException ioe) {
        //
      }
    }
  }

  public static void closeQuietly(OutputStream out) {
    if (out != null) {
      try {
        out.close();
      } catch (IOException ioe) {
        //
      }
    }
  }
}

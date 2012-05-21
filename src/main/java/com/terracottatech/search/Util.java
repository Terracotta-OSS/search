/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

public class Util {

  private static final int            DEL      = 0x7F;
  private static final char           ESCAPE   = '%';
  private static final Set<Character> ILLEGALS = new HashSet<Character>();

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
  }

  /**
   * Convert the given string into a unique and legal path for all file systems
   */
  public static String sanitizeCacheName(String name) {
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

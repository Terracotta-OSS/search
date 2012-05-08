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
import java.util.regex.Pattern;

public class Util {

  private static final Pattern LEGAL_CHARS = Pattern.compile("^[ \\w]$");

  /**
   * Remove any characters from a cache name that might not be legal for use in a directory/file name
   * NOTE: This may have the side effect of making the name no longer unique
   */
  public static String sanitizeCacheName(String name) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0, n = name.length(); i < n; i++) {
      char c = name.charAt(i);
      if (LEGAL_CHARS.matcher(String.valueOf(c)).matches()) {
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

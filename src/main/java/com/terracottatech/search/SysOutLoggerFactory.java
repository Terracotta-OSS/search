/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SysOutLoggerFactory implements LoggerFactory {

  private final boolean debugOn;

  public SysOutLoggerFactory() {
    this(false);
  }

  public SysOutLoggerFactory(boolean debugOn) {
    this.debugOn = debugOn;
  }

  @Override
  public Logger getLogger(String name) {
    return new SysOutLogger(name);
  }

  @Override
  public Logger getLogger(Class c) {
    return getLogger(c.getName());
  }

  private class SysOutLogger implements Logger {

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    private final String           name;

    public SysOutLogger(String name) {
      this.name = name;
    }

    @Override
    public void debug(Object message) {
      if (message instanceof Throwable) {
        debug(null, (Throwable) message);
      } else {
        debug(message, null);
      }
    }

    @Override
    public void debug(Object message, Throwable t) {
      if (isDebugEnabled()) {
        log("DEBUG", message, t);
      }
    }

    @Override
    public void error(Object message) {
      if (message instanceof Throwable) {
        error(null, (Throwable) message);
      } else {
        error(message, null);
      }
    }

    @Override
    public void error(Object message, Throwable t) {
      log("ERROR", message, t);
    }

    @Override
    public void fatal(Object message) {
      if (message instanceof Throwable) {
        fatal(null, (Throwable) message);
      } else {
        fatal(message, null);
      }
    }

    @Override
    public void fatal(Object message, Throwable t) {
      log("FATAL", message, t);
    }

    @Override
    public void info(Object message) {
      if (message instanceof Throwable) {
        info(null, (Throwable) message);
      } else {
        info(message, null);
      }
    }

    @Override
    public void info(Object message, Throwable t) {
      log("INFO", message, t);
    }

    @Override
    public void warn(Object message) {
      if (message instanceof Throwable) {
        warn(null, (Throwable) message);
      } else {
        warn(message, null);
      }
    }

    @Override
    public void warn(Object message, Throwable t) {
      log("WARN", message, t);
    }

    private synchronized void log(String level, Object message, Throwable t) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      pw.print(dateFormat.format(new Date()) + " [" + Thread.currentThread().getName() + "] " + level + " " + name
               + " - " + (message == null ? "" : message));
      if (t != null) {
        pw.println();
        t.printStackTrace(pw);
      }
      pw.close();
      System.out.println(sw.toString());
    }

    @Override
    public boolean isDebugEnabled() {
      return debugOn;
    }

    @Override
    public boolean isInfoEnabled() {
      return true;
    }

    @Override
    public String getName() {
      return name;
    }
  }

}

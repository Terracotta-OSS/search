/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 */
package com.terracottatech.search;

import java.util.HashMap;
import java.util.Map;


public enum ValueType {
  VALUE_ID,

  NULL,

  BOOLEAN,

  BYTE,

  CHAR,

  DOUBLE,

  FLOAT,

  INT,

  SHORT,

  LONG,

  STRING,

  DATE,

  SQL_DATE,

  BYTE_ARRAY,

  ENUM;

  private static final Map<Class<?>, ValueType> MAPPINGS = new HashMap<Class<?>, ValueType>(values().length);

  public static ValueType valueOf(Class<?> c) {
    ValueType type = MAPPINGS.get(c);
    if (type != null) { return type; }

    return c.isEnum() ? ENUM : null;
  }

  static {
    MAPPINGS.put(Boolean.class, BOOLEAN);
    MAPPINGS.put(Byte.class, BYTE);
    MAPPINGS.put(Character.class, CHAR);
    MAPPINGS.put(Double.class, DOUBLE);
    MAPPINGS.put(Float.class, FLOAT);
    MAPPINGS.put(Integer.class, INT);
    MAPPINGS.put(Long.class, LONG);
    MAPPINGS.put(Short.class, SHORT);
    MAPPINGS.put(String.class, STRING);
    MAPPINGS.put(java.util.Date.class, DATE);
    MAPPINGS.put(java.sql.Date.class, SQL_DATE);
    MAPPINGS.put(char.class, CHAR);
    MAPPINGS.put(int.class, INT);
    MAPPINGS.put(byte[].class, BYTE_ARRAY);
    MAPPINGS.put(long.class, LONG);
    MAPPINGS.put(byte.class, BYTE);
    MAPPINGS.put(boolean.class, BOOLEAN);
    MAPPINGS.put(float.class, FLOAT);
    MAPPINGS.put(double.class, DOUBLE);
    MAPPINGS.put(short.class, SHORT);
  }

}

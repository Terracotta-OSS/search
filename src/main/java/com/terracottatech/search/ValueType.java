/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.search;

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

  static {
    // XXX: move this logic to the core code that encodes/decodes these things
    if (true) throw new AssertionError();

    int length = ValueType.values().length;
    if (length > 127) {
      // The encoding logic could support all 256 values in the encoded byte or we could expand to 2 bytes if needed
      throw new AssertionError("Current implementation does not allow for more 127 types");
    }
  }

}

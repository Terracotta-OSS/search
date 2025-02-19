/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 */
package com.terracottatech.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public abstract class AbstractNVPair implements NVPair {

  // private static final AbstractNVPair TEMPLATE = new Template();
  // private static final ValueType[] ALL_TYPES = ValueType.values();

  private final String name;

  AbstractNVPair(String name) {
    this.name = name;
  }

  @Override
  public final String getName() {
    return name;
  }

  @Override
  public final String toString() {
    return getType() + "(" + getName() + "," + valueAsString() + ")";
  }

  @Override
  public abstract NVPair cloneWithNewName(String newName);

  @Override
  public final boolean equals(Object obj) {
    if (obj instanceof AbstractNVPair) {
      NVPair other = (NVPair) obj;
      if (other.getName().equals(getName())) { return basicEquals(other); }
    }
    return false;
  }

  @Override
  public int compareTo(NVPair other) {
    boolean thisNull = getObjectValue() == null, otherNull = other.getObjectValue() == null;
    if (!(thisNull || otherNull)) return ((Comparable) getObjectValue()).compareTo(other.getObjectValue());

    return otherNull ? (thisNull ? 0 : 1) : -1;
  }

  abstract boolean basicEquals(NVPair other);

  @Override
  public final int hashCode() {
    return getType().hashCode() ^ name.hashCode() ^ valueAsString().hashCode();
  }

  // XXX: make this non-public when possible
  @Override
  public abstract String valueAsString();

  @Override
  public abstract ValueType getType();

  // private static class Template extends AbstractNVPair {
  //
  // Template() {
  // super("");
  // }
  //
  // @Override
  // public String valueAsString() {
  // throw new AssertionError();
  // }
  //
  // @Override
  // public ValueType getType() {
  // throw new AssertionError();
  // }
  //
  // @Override
  // boolean basicEquals(NVPair other) {
  // throw new AssertionError();
  // }
  //
  // public Object getObjectValue() {
  // throw new AssertionError();
  // }
  //
  // @Override
  // public NVPair cloneWithNewName(String newName) {
  // throw new AssertionError();
  // }
  //
  // public NVPair cloneWithNewValue(Object newValue) {
  // throw new AssertionError();
  // }
  // }

  public static class ByteNVPair extends AbstractNVPair {
    private final byte value;

    public ByteNVPair(String name, byte value) {
      super(name);
      this.value = value;
    }

    public byte getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public ValueType getType() {
      return ValueType.BYTE;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof ByteNVPair) { return value == ((ByteNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new ByteNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new ByteNVPair(getName(), (Byte) newValue);
    }
  }

  public static class BooleanNVPair extends AbstractNVPair {
    private final boolean value;

    public BooleanNVPair(String name, boolean value) {
      super(name);
      this.value = value;
    }

    public boolean getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public ValueType getType() {
      return ValueType.BOOLEAN;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof BooleanNVPair) { return value == ((BooleanNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new BooleanNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new BooleanNVPair(getName(), (Boolean) newValue);
    }
  }

  public static class CharNVPair extends AbstractNVPair {
    private final char value;

    public CharNVPair(String name, char value) {
      super(name);
      this.value = value;
    }

    public char getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    public ValueType getType() {
      return ValueType.CHAR;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof CharNVPair) { return value == ((CharNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new CharNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new CharNVPair(getName(), (Character) newValue);
    }
  }

  public static class DoubleNVPair extends AbstractNVPair {
    private final double value;

    public DoubleNVPair(String name, double value) {
      super(name);
      this.value = value;
    }

    public double getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    public ValueType getType() {
      return ValueType.DOUBLE;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof DoubleNVPair) { return value == ((DoubleNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new DoubleNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new DoubleNVPair(getName(), (Double) newValue);
    }
  }

  public static class FloatNVPair extends AbstractNVPair {
    private final float value;

    public FloatNVPair(String name, float value) {
      super(name);
      this.value = value;
    }

    public float getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    public ValueType getType() {
      return ValueType.FLOAT;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof FloatNVPair) { return value == ((FloatNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new FloatNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new FloatNVPair(getName(), (Float) newValue);
    }
  }

  public static class IntNVPair extends AbstractNVPair {
    private final int value;

    public IntNVPair(String name, int value) {
      super(name);
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    public ValueType getType() {
      return ValueType.INT;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof IntNVPair) { return value == ((IntNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new IntNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new IntNVPair(getName(), (Integer) newValue);
    }
  }

  public static class ShortNVPair extends AbstractNVPair {
    private final short value;

    public ShortNVPair(String name, short value) {
      super(name);
      this.value = value;
    }

    public short getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    public ValueType getType() {
      return ValueType.SHORT;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof ShortNVPair) { return value == ((ShortNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new ShortNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new ShortNVPair(getName(), (Short) newValue);
    }
  }

  public static class LongNVPair extends AbstractNVPair {
    private final long value;

    public LongNVPair(String name, long value) {
      super(name);
      this.value = value;
    }

    public long getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return String.valueOf(value);
    }

    @Override
    public ValueType getType() {
      return ValueType.LONG;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof LongNVPair) { return value == ((LongNVPair) obj).value; }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new LongNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new LongNVPair(getName(), (Long) newValue);
    }
  }

  public static class StringNVPair extends AbstractNVPair {
    private final String value;

    public StringNVPair(String name, String value) {
      super(name);
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return value;
    }

    @Override
    public ValueType getType() {
      return ValueType.STRING;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof StringNVPair) { return value.equals(((StringNVPair) obj).value); }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new StringNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new StringNVPair(getName(), (String) newValue);
    }
  }

  public static class ByteArrayNVPair extends AbstractNVPair {
    private final byte[] value;

    public ByteArrayNVPair(String name, byte[] value) {
      super(name);
      this.value = value;
    }

    public byte[] getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      List<Byte> list = new ArrayList<Byte>(value.length);
      for (byte b : value) {
        list.add(b);
      }
      return list.toString();
    }

    @Override
    public ValueType getType() {
      return ValueType.BYTE_ARRAY;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof ByteArrayNVPair) { return Arrays.equals(value, ((ByteArrayNVPair) obj).value); }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new ByteArrayNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new ByteArrayNVPair(getName(), (byte[]) newValue);
    }
  }

  public static class DateNVPair extends AbstractNVPair {
    private final Date value;

    public DateNVPair(String name, Date value) {
      super(name);
      this.value = value;
    }

    public Date getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return value.toString();
    }

    @Override
    public ValueType getType() {
      return ValueType.DATE;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof DateNVPair) { return value.equals(((DateNVPair) obj).value); }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new DateNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new DateNVPair(getName(), (Date) newValue);
    }
  }

  public static class SqlDateNVPair extends AbstractNVPair {
    private final java.sql.Date value;

    public SqlDateNVPair(String name, java.sql.Date value) {
      super(name);
      this.value = value;
    }

    public java.sql.Date getValue() {
      return value;
    }

    @Override
    public Object getObjectValue() {
      return value;
    }

    @Override
    public String valueAsString() {
      return value.toString();
    }

    @Override
    public ValueType getType() {
      return ValueType.SQL_DATE;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof SqlDateNVPair) { return value.equals(((SqlDateNVPair) obj).value); }
      return false;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new SqlDateNVPair(newName, value);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new SqlDateNVPair(getName(), (java.sql.Date) newValue);
    }
  }

  public static class EnumNVPair extends AbstractNVPair implements NVPairEnum {

    private final String className;
    private final int    ordinal;

    public EnumNVPair(String name, Enum e) {
      this(name, e.getDeclaringClass().getName(), e.ordinal());
    }

    @Override
    public Object getObjectValue() {
      try {
        return Class.forName(className, false, Thread.currentThread().getContextClassLoader()).getEnumConstants()[ordinal];
      } catch (ClassNotFoundException e) {
        // XXX: Should CNFE be part of getObjectValue() signature? This runtime smells bad
        throw new RuntimeException(e);
      }
    }

    public EnumNVPair(String name, String className, int ordinal) {
      super(name);
      this.className = className;
      this.ordinal = ordinal;
    }

    @Override
    boolean basicEquals(NVPair obj) {
      if (obj instanceof EnumNVPair) {
        EnumNVPair other = (EnumNVPair) obj;
        return ordinal == other.ordinal && className.equals(other.className);
      }

      return false;
    }

    @Override
    public String valueAsString() {
      return className + "(" + ordinal + ")";
    }

    @Override
    public ValueType getType() {
      return ValueType.ENUM;
    }

    @Override
    public int getOrdinal() {
      return ordinal;
    }

    @Override
    public String getClassName() {
      return className;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new EnumNVPair(newName, className, ordinal);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new EnumNVPair(getName(), (Enum) newValue);
    }

    @Override
    public int compareTo(NVPair other) {
      if (other instanceof NullNVPair) return 1; // move nulls to front
      if (!(other instanceof EnumNVPair && getName().equals(other.getName()) && className
          .equals(((EnumNVPair) other).className))) throw new IllegalArgumentException(
                                                                                       "Incompatible value given to compareTo: "
                                                                                           + other);
      return ordinal - ((EnumNVPair) other).ordinal;
    }

  }

  public static class NullNVPair extends AbstractNVPair {

    public NullNVPair(String name) {
      super(name);
    }

    @Override
    public Object getObjectValue() {
      return null;
    }

    @Override
    boolean basicEquals(NVPair other) {
      return other instanceof NullNVPair;
    }

    @Override
    public String valueAsString() {
      // XXX: is this a good return value?
      return "null";
    }

    @Override
    public ValueType getType() {
      return ValueType.NULL;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new NullNVPair(newName);
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      if (newValue != null) { throw new IllegalArgumentException(); }
      return this;
    }

    @Override
    public int compareTo(NVPair other) {
      if (!getName().equals(other.getName())) throw new IllegalArgumentException("Names don't match: " + other);
      return basicEquals(other) ? 0 : -1;
    }
  }

  public static class ValueIdNVPair extends AbstractNVPair {

    private final ValueID id;

    public ValueIdNVPair(String name, ValueID oid) {
      super(name);
      this.id = oid;
    }

    @Override
    public Object getObjectValue() {
      return id;
    }

    public ValueID getValue() {
      return id;
    }

    @Override
    public NVPair cloneWithNewName(String newName) {
      return new ValueIdNVPair(newName, id);
    }

    @Override
    boolean basicEquals(NVPair other) {
      if (other instanceof ValueIdNVPair) { return id.equals(((ValueIdNVPair) other).id); }
      return false;
    }

    @Override
    public String valueAsString() {
      return id.toString();
    }

    @Override
    public ValueType getType() {
      return ValueType.VALUE_ID;
    }

    @Override
    public NVPair cloneWithNewValue(Object newValue) {
      return new ValueIdNVPair(getName(), (ValueID) newValue);
    }
  }

  public static NVPair createNVPair(String name, Object value, ValueType type) {
    if (value == null) return new NullNVPair(name);

    if (ValueType.ENUM.equals(type)) { return enumPairFromString(name, (String) value); }

    return AbstractNVPair.createNVPair(name, value);
  }

  public static EnumNVPair enumPairFromString(String name, String enumString) {
    String className = enumString.substring(0, enumString.length() - 10);
    int ordinal = Integer.parseInt(enumString.substring(enumString.length() - 10));
    return new EnumNVPair(name, className, ordinal);
  }

  public static String enumStorageString(EnumNVPair enumPair) {
    return enumStorageString(enumPair.getClassName(), enumPair.getOrdinal());
  }

  public static String enumStorageString(Enum e) {
    return enumStorageString(e.getDeclaringClass().getName(), e.ordinal());
  }

  private static String enumStorageString(String className, int ordinal) {
    StringBuilder sb = new StringBuilder(className.length() + 10);
    sb.append(className);

    String ordinalString = String.valueOf(ordinal);
    for (int i = ordinalString.length(); i < 10; i++) {
      sb.append('0');
    }

    sb.append(ordinalString);
    return sb.toString();
  }

  public static NVPair createNVPair(String attributeName, Object value) {
    if (value == null) { return new NullNVPair(attributeName); }

    if (value instanceof Byte) {
      return new ByteNVPair(attributeName, (Byte) value);
    } else if (value instanceof Boolean) {
      return new BooleanNVPair(attributeName, (Boolean) value);
    } else if (value instanceof Character) {
      return new CharNVPair(attributeName, (Character) value);
    } else if (value instanceof Double) {
      return new DoubleNVPair(attributeName, (Double) value);
    } else if (value instanceof Float) {
      return new FloatNVPair(attributeName, (Float) value);
    } else if (value instanceof Integer) {
      return new IntNVPair(attributeName, (Integer) value);
    } else if (value instanceof Short) {
      return new ShortNVPair(attributeName, (Short) value);
    } else if (value instanceof Long) {
      return new LongNVPair(attributeName, (Long) value);
    } else if (value instanceof String) {
      return new StringNVPair(attributeName, (String) value);
    } else if (value instanceof byte[]) {
      return new ByteArrayNVPair(attributeName, (byte[]) value);
    } else if (value instanceof java.sql.Date) {
      // this one must come before regular java.util.Date
      return new SqlDateNVPair(attributeName, (java.sql.Date) value);
    } else if (value instanceof Date) {
      return new DateNVPair(attributeName, (Date) value);
    } else if (value instanceof ValueID) {
      return new ValueIdNVPair(attributeName, (ValueID) value);
    } else if (value instanceof Enum) { return new EnumNVPair(attributeName, (Enum) value); }

    throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
  }
}

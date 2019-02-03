/*
 * @(#)AtomicKeyType.java   1.0   Apr 21, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import minibase.util.Convert;

/**
 * The class of {@link SearchKeyType} that are allowed in the {@link CompositeKeyType}.
 *
 * @author Raffael Wagner
 * @version 1.0
 */
public abstract class AtomicKeyType extends SearchKeyType {

   /** The long key type. */
   public static final LongKeyType LONG = new LongKeyType();
   /** The integer key type. */
   public static final IntegerKeyType INTEGER = new IntegerKeyType();
   /** The short key type. */
   public static final ShortKeyType SHORT = new ShortKeyType();
   /** The byte key type. */
   public static final ByteKeyType BYTE = new ByteKeyType();
   /** The float key type. */
   public static final FloatKeyType FLOAT = new FloatKeyType();
   /** The double key type. */
   public static final DoubleKeyType DOUBLE = new DoubleKeyType();
   /** The timestamp key type. */
   public static final TimestampKeyType TIMESTAMP = new TimestampKeyType();
   /** The date key type. */
   public static final DateKeyType DATE = new DateKeyType();
   /** The time key type. */
   public static final TimeKeyType TIME = new TimeKeyType();

   /**
    * Creates an Atomic Key Type of given size.
    * @param kind the kind of the type
    * @param length the length of the type
    * @param keyLength the size of the key
    */
   public AtomicKeyType(final Kind kind, final int length, final int keyLength) {
      super(kind, length, keyLength);
   }

   /**
    * Read the value from the data.
    * @param data the data that is read from
    * @param offset the position to start the read
    * @return the value
    */
   public abstract Object readObject(byte[] data, int offset);


   @Override
   public SearchKey readSearchKey(final byte[] data, final int offset) {
      return new SearchKey(this, this.readObject(data, offset));
   }

   /**
    * Recursive helper.
    *
    * @param sb string builder
    */
   abstract void toString(StringBuilder sb);

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(AtomicKeyType.class.getSimpleName()).append('[');
      this.toString(sb);
      return sb.append(']').toString();
   }

   @Override
   public int hashCode() {
      return this.getID();
   }

   @Override
   public boolean equals(final Object obj) {
      return obj != null && this.getClass() == obj.getClass();
   }

   @Override
   public void writeTo(final byte[] data, final int offset) {
      data[offset] = this.getID();
   }

   /**
    * The class for the key type Integer.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class IntegerKeyType extends AtomicKeyType {

      /**
       * Creates an Integer Key Type.
       */
      private IntegerKeyType() {
         super(Kind.INTEGER, 1, Integer.BYTES);
      }

      /**
       * @return an IntegerKeyType instance
       */
      public static IntegerKeyType getInstance() {
         return AtomicKeyType.INTEGER;
      }

      @Override
      public Integer readObject(final byte[] data, final int offset) {
         return Convert.readInt(data, offset);
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeInt(data, offset, (Integer) values[index]);
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         return (Integer) values[index];
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Integer) values1[index]).compareTo((Integer) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Integer) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Integer.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Long.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class LongKeyType extends AtomicKeyType {

      /**
       * Creates a Long Key Type.
       */
      private LongKeyType() {
         super(Kind.LONG, 1, Long.BYTES);
      }

      /**
       * @return a LongKeyType instance.
       */
      public static LongKeyType getInstance() {
         return AtomicKeyType.LONG;
      }

      @Override
      public Long readObject(final byte[] data, final int offset) {
         return Convert.readLong(data, offset);
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeLong(data, offset, (Long) values[index]);
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         final long l = (Long) values[index];
         return (int) l  ^ (int) (l >>> 32);
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Long) values1[index]).compareTo((Long) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Long) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Long.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Short.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class ShortKeyType extends AtomicKeyType {

      /**
       * Creates a Short Key Type.
       */
      private ShortKeyType() {
         super(Kind.SHORT, 1, Short.BYTES);
      }

      /**
       * @return a ShortKeyType instance.
       */
      public static ShortKeyType getInstance() {
         return AtomicKeyType.SHORT;
      }

      @Override
      public Short readObject(final byte[] data, final int offset) {
         return Convert.readShort(data, offset);
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeShort(data, offset, (Short) values[index]);
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         return (Short) values[index];
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Short) values1[index]).compareTo((Short) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Short) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Short.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Byte.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class ByteKeyType extends AtomicKeyType {

      /**
       * Creates a Byte Key Type.
       */
      private ByteKeyType() {
         super(Kind.BYTE, 1, Byte.BYTES);
      }

      /**
       * @return a BytetKeyType instance.
       */
      public static ByteKeyType getInstance() {
         return AtomicKeyType.BYTE;
      }

      @Override
      public Byte readObject(final byte[] data, final int offset) {
         return data[offset];
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeByte(data, offset, (Byte) values[index]);
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         return (Byte) values[index];
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Byte) values1[index]).compareTo((Byte) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Byte) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Byte.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Float.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class FloatKeyType extends AtomicKeyType {

      /**
       * Creates a Float Key Type.
       */
      private FloatKeyType() {
         super(Kind.FLOAT, 1, Float.BYTES);
      }

      /**
       * @return a FloatKeyType instance.
       */
      public static FloatKeyType getInstance() {
         return AtomicKeyType.FLOAT;
      }

      @Override
      public Float readObject(final byte[] data, final int offset) {
         return Convert.readFloat(data, offset);
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeFloat(data, offset, (Float) values[index]);
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         return Float.floatToIntBits((Float) values[index]);
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Float) values1[index]).compareTo((Float) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Float) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Float.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Double.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class DoubleKeyType extends AtomicKeyType {

      /**
       * Creates a Double Key Type.
       */
      private DoubleKeyType() {
         super(Kind.DOUBLE, 1, Double.BYTES);
      }

      /**
       * @return a DoubleKeyType instance.
       */
      public static DoubleKeyType getInstance() {
         return DOUBLE;
      }

      @Override
      public Double readObject(final byte[] data, final int offset) {
         return Convert.readDouble(data, offset);
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeDouble(data, offset, (Double) values[index]);
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         final long l = Double.doubleToLongBits((Double) values[index]);
         return (int) l  ^ (int) (l >>> 32);
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Double) values1[index]).compareTo((Double) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Double) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Double.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Timestamp.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class TimestampKeyType extends AtomicKeyType {

      /**
       * Creates a Timestamp Key Type.
       */
      private TimestampKeyType() {
         super(Kind.TIMESTAMP, 1, Long.BYTES);
      }

      /**
       * @return a TimestampKeyType instance.
       */
      public static TimestampKeyType getInstance() {
         return AtomicKeyType.TIMESTAMP;
      }

      @Override
      public Timestamp readObject(final byte[] data, final int offset) {
         return new Timestamp(Convert.readLong(data, offset));
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeLong(data, offset, ((Timestamp) values[index]).getTime());
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         final long l = ((Timestamp) values[index]).getTime();
         return (int) l  ^ (int) (l >>> 32);
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Timestamp) values1[index]).compareTo((Timestamp) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Timestamp) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Timestamp.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Time.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class TimeKeyType extends AtomicKeyType {

      /**
       * Creates a Time Key Type.
       */
      private TimeKeyType() {
         super(Kind.TIME, 1, Long.BYTES);
      }

      /**
       * @return a TimeKeyType instance.
       */
      public static TimeKeyType getInstance() {
         return AtomicKeyType.TIME;
      }

      @Override
      public Time readObject(final byte[] data, final int offset) {
         return new Time(Convert.readLong(data, offset));
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeLong(data, offset, ((Time) values[index]).getTime());
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         final long l = ((Time) values[index]).getTime();
         return (int) l  ^ (int) (l >>> 32);
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Time) values1[index]).compareTo((Time) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Time) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Time.class.getSimpleName());
      }
   }

   /**
    * The class for the key type Date.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class DateKeyType extends AtomicKeyType {

      /**
       * Creates a Time Key Type.
       */
      private DateKeyType() {
         super(Kind.DATE, 1, Long.BYTES);
      }

      /**
       * @return a TimeKeyType instance.
       */
      public static DateKeyType getInstance() {
         return AtomicKeyType.DATE;
      }

      @Override
      public Date readObject(final byte[] data, final int offset) {
         return new Date(Convert.readLong(data, offset));
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeLong(data, offset, ((Date) values[index]).getTime());
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         final long l = ((Date) values[index]).getTime();
         return (int) l  ^ (int) (l >>> 32);
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((Date) values1[index]).compareTo((Date) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return ((Date) values[index]).toString();
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(Date.class.getSimpleName());
      }
   }

   /**
    * The class for the key type String.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public static final class StringKeyType extends AtomicKeyType {

      /**
       * Creates a String Key Type of given size.
       * @param length the size of the key
       */
      private StringKeyType(final int length) {
         super(Kind.STRING, 3, length);
      }

      /**
       * @param length the length of the keys.
       * @return an IntegerKeyType instance
       */
      public static StringKeyType getInstance(final int length) {
         return new StringKeyType(length);
      }

      @Override
      public int hashCode() {
         return this.getID() + 31 * this.getKeyLength();
      }

      @Override
      public boolean equals(final Object obj) {
         if (this == obj) {
            return true;
         }
         if (obj == null || this.getClass() != obj.getClass()) {
            return false;
         }
         final StringKeyType other = (StringKeyType) obj;
         return this.getKeyLength() == other.getKeyLength();
      }

      @Override
      public String readObject(final byte[] data, final int offset) {
         return Convert.readString(data, offset, this.getKeyLength());
      }

      @Override
      public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
         Convert.writeString(data, offset, (String) values[index], this.getKeyLength());
      }

      @Override
      public int getHashCode(final Object[] values, final int index) {
         return values[index].hashCode();
      }

      @Override
      public int compareValues(final Object[] values1, final Object[] values2, final int index) {
         return ((String) values1[index]).compareTo((String) values2[index]);
      }

      @Override
      public String getString(final Object[] values, final int index) {
         return (String) values[index];
      }

      @Override
      public void writeTo(final byte[] data, final int offset) {
         data[offset] = this.getID();
         Convert.writeShort(data, offset + 1, (short) this.getKeyLength());
      }

      @Override
      void toString(final StringBuilder sb) {
         sb.append(String.class.getSimpleName()).append('(').append(this.getLength()).append(')');
      }
   }
}

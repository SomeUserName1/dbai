/*
 * @(#)Convert.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.util;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/**
 * Provides conversion routines for getting and setting data in byte arrays.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class Convert {

   /** The UTF-8 charset. */
   private static final Charset UTF_8 = Charset.availableCharsets().get("UTF-8");

   /** Hidden default constructor. */
   private Convert() {
      throw new AssertionError();
   }

   /**
    * Reads from the given byte array at the specified position, and converts it into a byte.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read byte
    */
   public static byte readByte(final byte[] data, final int pos) {
      return (byte) read8(data, pos);
   }

   /**
    * Writes a byte into the given byte array at the specified position.
    *
    * @param data
    *           the data to write
    * @param pos
    *           position to write at
    * @param value
    *           the value to write
    */
   public static void writeByte(final byte[] data, final int pos, final byte value) {
      write8(data, pos, value);
   }

   /**
    * Reads from the given byte array at the specified position, and converts it into a Unicode character.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read char value
    */
   public static char readChar(final byte[] data, final int pos) {
      return (char) read16(data, pos);
   }

   /**
    * Writes a unicode character into the given byte array at the specified position.
    *
    * @param data
    *           the data to write
    * @param pos
    *           position to write at
    * @param value
    *           the value to write
    */
   public static void writeChar(final byte[] data, final int pos, final char value) {
      write16(data, pos, value);
   }

   /**
    * Reads from the given byte array at the specified position, and converts it into a short.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read value
    */
   public static short readShort(final byte[] data, final int pos) {
      return (short) read16(data, pos);
   }

   /**
    * Writes a short into the given byte array at the specified position.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param value
    *           value to write
    */
   public static void writeShort(final byte[] data, final int pos, final short value) {
      write16(data, pos, value);
   }

   /**
    * Reads from the given byte array at the specified position, and converts it into an integer.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read value
    */
   public static int readInt(final byte[] data, final int pos) {
      return read32(data, pos);
   }

   /**
    * Writes an integer into the given byte array at the specified position.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param value
    *           value to write
    */
   public static void writeInt(final byte[] data, final int pos, final int value) {
      write32(data, pos, value);
   }

   /**
    * Reads from the given byte array at the specified position, and converts it to a float.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read value
    */
   public static float readFloat(final byte[] data, final int pos) {
      // let java do the IEEE 754 conversion
      return Float.intBitsToFloat(read32(data, pos));
   }

   /**
    * Writes a float into the given byte array at the specified position.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param value
    *           value to write
    */
   public static void writeFloat(final byte[] data, final int pos, final float value) {
      // let java do the IEEE 754 conversion
      write32(data, pos, Float.floatToRawIntBits(value));
   }

   /**
    * Reads from the given byte array at the specified position, and converts it into a long.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read value
    */
   public static long readLong(final byte[] data, final int pos) {
      return read64(data, pos);
   }

   /**
    * Writes a long into the given byte array at the specified position.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param value
    *           value to write
    */
   public static void writeLong(final byte[] data, final int pos, final long value) {
      write64(data, pos, value);
   }

   /**
    * Reads from the given byte array at the specified position, and converts it to a double.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read value
    */
   public static double readDouble(final byte[] data, final int pos) {
      // let java do the IEEE 754 conversion
      return Double.longBitsToDouble(read64(data, pos));
   }

   /**
    * Writes a double into the given byte array at the specified position.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param value
    *           value to write
    */
   public static void writeDouble(final byte[] data, final int pos, final double value) {
      // let java do the IEEE 754 conversion
      write64(data, pos, Double.doubleToRawLongBits(value));
   }

   /**
    * Reads from the given byte array at the specified position, and converts it to a byte array of given
    * length.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           pos position to read at
    * @param length
    *           number of bytes to read
    * @return the read array
    */
   private static byte[] readByteArray(final byte[] data, final int pos, final int length) {
      final byte[] sub = new byte[Math.min(length, data.length - pos)];
      System.arraycopy(data, pos, sub, 0, sub.length);
      return sub;
   }

   /**
    * Writes a byte array into the given byte array at the specified position.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param value
    *           bytes to write
    */
   private static void writeByteArray(final byte[] data, final int pos, final byte[] value) {
      System.arraycopy(value, 0, data, pos, Math.min(value.length, data.length - pos));
   }

   /**
    * Reads a string from the given byte array.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @param length
    *           number of bytes to read
    * @return the read string
    */
   public static String readString(final byte[] data, final int pos, final int length) {
      return fromUTF8Bytes(readByteArray(data, pos, length));
   }

   /**
    * Writes a given string to the given byte array.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param str
    *           string to write
    * @param length
    *           maximum number of bytes to write
    */
   public static void writeString(final byte[] data, final int pos, final String str, final int length) {
      writeByteArray(data, pos, toUTF8Bytes(str, length));
   }

   /**
    * Reads a date from the given byte array.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read date
    */
   public static Date readDate(final byte[] data, final int pos) {
      // TODO add TimeZone
      final long days = read24(data, pos);
      final LocalDate localDate = LocalDate.ofEpochDay(days);
      return Date.valueOf(localDate);
   }

   /**
    * Reads a date from the given byte array and returns it as Days since 1970-01-01.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return days since 1970-01-01
    */
   public static int readDateDays(final byte[] data, final int pos) {
      return read24(data, pos);
   }

   /**
    * Writes a given date to the given byte array.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param date
    *           date to write
    */
   public static void writeDate(final byte[] data, final int pos, final Date date) {
      // TODO add TimeZone
      final LocalDate localDate = date.toLocalDate();
      final int days = (int) localDate.toEpochDay();
      write24(data, pos, days);
   }

   /**
    * Reads a time from the given byte array.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read time
    */
   public static Time readTime(final byte[] data, final int pos) {
      final long millisSinceMidnight = read40(data, pos);
      final LocalTime localTime = LocalTime.MIDNIGHT.plus(millisSinceMidnight, ChronoUnit.MILLIS);
      return Time.valueOf(localTime);
   }

   /**
    * Reads a time from the given byte array and returns it as milliseconds since midnight.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return milliseconds since midnight
    */
   public static long readTimeMillis(final byte[] data, final int pos) {
      return read40(data, pos);
   }

   /**
    * Writes a given time to the given byte array.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param time
    *           time to write
    */
   public static void writeTime(final byte[] data, final int pos, final Time time) {

      // 5 bytes can be used.
      // get milliseconds past since midnight.
      final Duration duration = Duration.between(LocalTime.MIDNIGHT, time.toLocalTime());
      final long millisSinceMidnight = duration.toMillis();
      write40(data, pos, millisSinceMidnight);
   }

   /**
    * Reads a date time from the given byte array.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           position to read at
    * @return the read date time
    */
   public static Timestamp readTimestamp(final byte[] data, final int pos) {
      final long millis = readLong(data, pos);
      return new Timestamp(millis);
   }

   /**
    * Writes a given date time to the given byte array.
    *
    * @param data
    *           byte array to write to
    * @param pos
    *           position to write at
    * @param datetime
    *           string to date time
    */
   public static void writeTimestamp(final byte[] data, final int pos, final Timestamp datetime) {
      final long millis = datetime.getTime();
      writeLong(data, pos, millis);
   }

   /**
    * Reads a 8-bit integer from the given array at the given offset.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           reading position
    * @return the read 8-bit integer
    */
   private static int read8(final byte[] data, final int pos) {
      return data[pos] & 0xFF;
   }

   /**
    * Writes a 8-bit integer to the given byte array at the given position.
    *
    * @param data
    *           the array to write to
    * @param pos
    *           the position to write at
    * @param val
    *           the value to write
    */
   private static void write8(final byte[] data, final int pos, final int val) {
      data[pos + 0] = (byte) val;
   }

   /**
    * Reads a 16-bit integer from the given array at the given offset.
    *
    * @param data
    *           byte array to read from
    * @param pos
    *           reading position
    * @return the read 16-bit integer
    */
   private static int read16(final byte[] data, final int pos) {
      return (data[pos] & 0xFF) << 8 | data[pos + 1] & 0xFF;
   }

   /**
    * Writes a 16-bit integer to the given byte array at the given position.
    *
    * @param data
    *           the array to write to
    * @param pos
    *           the position to write at
    * @param val
    *           the value to write
    */
   private static void write16(final byte[] data, final int pos, final int val) {
      data[pos + 0] = (byte) (val >>> 8);
      data[pos + 1] = (byte) val;
   }

   /**
    * Reads a 24-bit integer from the given byte array at the given offset.
    *
    * @param data
    *           the array to read from
    * @param pos
    *           the position to read at
    * @return the read 24-bit integer
    */
   private static int read24(final byte[] data, final int pos) {
      // shift to the right is used to handle the signed integer.
      return ((data[pos] & 0xFF) << 24 | (data[pos + 1] & 0xFF) << 16 | (data[pos + 2] & 0xFF) << 8) >> 8;
   }

   /**
    * Writes a 24-bit integer to the given byte array at the given offset.
    *
    * @param data
    *           the byte array to write to
    * @param pos
    *           the position to write at
    * @param value
    *           the value to write
    */
   private static void write24(final byte[] data, final int pos, final int value) {
      data[pos + 0] = (byte) (value >> 16);
      data[pos + 1] = (byte) (value >> 8);
      data[pos + 2] = (byte) value;
   }

   /**
    * Reads a 32-bit integer from the given byte array at the given offset.
    *
    * @param data
    *           the array to read from
    * @param pos
    *           the position to read at
    * @return the read 32-bit integer
    */
   private static int read32(final byte[] data, final int pos) {
      return (data[pos + 0] & 0xFF) << 24 | (data[pos + 1] & 0xFF) << 16 | (data[pos + 2] & 0xFF) << 8
            | (data[pos + 3] & 0xFF) << 0;
   }

   /**
    * Writes a 32-bit integer to the given byte array at the given offset.
    *
    * @param data
    *           the byte array to write to
    * @param pos
    *           the position to write at
    * @param value
    *           the value to write
    */
   private static void write32(final byte[] data, final int pos, final int value) {
      data[pos + 0] = (byte) (value >>> 24);
      data[pos + 1] = (byte) (value >>> 16);
      data[pos + 2] = (byte) (value >>> 8);
      data[pos + 3] = (byte) (value >>> 0);
   }

   /**
    * Reads a 40-bit integer from the given byte array at the given offset.
    *
    * @param data
    *           the array to read from
    * @param pos
    *           the position to read at
    * @return the read 40-bit integer
    */
   private static long read40(final byte[] data, final int pos) {
      // shift to the right is used to handle the signed integer.
      return ((data[pos + 0] & 0xFFL) << 56 | (data[pos + 1] & 0xFFL) << 48 | (data[pos + 2] & 0xFFL) << 40
            | (data[pos + 3] & 0xFFL) << 32 | (data[pos + 4] & 0xFFL) << 24) >> 24;
   }

   /**
    * Writes a 40-bit integer to the given byte array at the given offset.
    *
    * @param data
    *           the byte array to write to
    * @param pos
    *           the position to write at
    * @param value
    *           the value to write
    */
   private static void write40(final byte[] data, final int pos, final long value) {
      data[pos + 0] = (byte) (value >>> 32);
      data[pos + 1] = (byte) (value >>> 24);
      data[pos + 2] = (byte) (value >>> 16);
      data[pos + 3] = (byte) (value >>> 8);
      data[pos + 4] = (byte) value;
   }

   /**
    * Reads a 64-bit integer from the given byte array at the given offset.
    *
    * @param data
    *           the array to read from
    * @param pos
    *           the position to read at
    * @return the read 64-bit integer
    */
   private static long read64(final byte[] data, final int pos) {
      return (data[pos + 0] & 0xFFL) << 56 | (data[pos + 1] & 0xFFL) << 48 | (data[pos + 2] & 0xFFL) << 40
            | (data[pos + 3] & 0xFFL) << 32 | (data[pos + 4] & 0xFFL) << 24 | (data[pos + 5] & 0xFFL) << 16
            | (data[pos + 6] & 0xFFL) << 8 | (data[pos + 7] & 0xFFL) << 0;
   }

   /**
    * Writes a 64-bit integer to the given byte array at the given offset.
    *
    * @param data
    *           the byte array to write to
    * @param pos
    *           the position to write at
    * @param value
    *           the value to write
    */
   private static void write64(final byte[] data, final int pos, final long value) {
      data[pos + 0] = (byte) (value >>> 56);
      data[pos + 1] = (byte) (value >>> 48);
      data[pos + 2] = (byte) (value >>> 40);
      data[pos + 3] = (byte) (value >>> 32);
      data[pos + 4] = (byte) (value >>> 24);
      data[pos + 5] = (byte) (value >>> 16);
      data[pos + 6] = (byte) (value >>> 8);
      data[pos + 7] = (byte) (value >>> 0);
   }

   /**
    * Converts the given string to an array of UTF-8 encoded bytes.
    *
    * @param str
    *           string to encode
    * @param length
    *           maximum number of bytes
    * @return the encoded string
    */
   private static byte[] toUTF8Bytes(final String str, final int length) {
      final byte[] bytes = Arrays.copyOf(str.getBytes(UTF_8), length);
      for (int i = length; --i >= 0;) {
         // truncate the string at a character boundary
         if (bytes[i] >>> 6 != 0x02) {
            break;
         }
         bytes[i] = 0;
      }
      return bytes;
   }

   /**
    * Converts the given array of UTF-8 encoded bytes to the corresponding string.
    *
    * @param utf8
    *           UTF-8 byte array
    * @return the corresponding string
    */
   private static String fromUTF8Bytes(final byte[] utf8) {
      return new String(utf8, UTF_8).trim();
   }

   /**
    * Converts a 32-bit integer to a 4-byte byte array.
    *
    * @param value
    *           the value to write
    * @return byte array
    */
   public static byte[] getBytes(final int value) {
      final byte[] data = new byte[4];
      data[0] = (byte) (value >>> 24);
      data[1] = (byte) (value >>> 16);
      data[2] = (byte) (value >>> 8);
      data[3] = (byte) (value >>> 0);
      return data;
   }
}

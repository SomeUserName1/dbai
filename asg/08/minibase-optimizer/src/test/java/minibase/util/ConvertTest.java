/*
 * @(#)ConvertTest.java   1.0   Sep 18, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.util;

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link Convert}.
 *
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 * @version 1.0
 */
public class ConvertTest {

   /**
    * Byte-array to simulate a page.
    */
   private byte[] data;

   /**
    * Clear the test page.
    */
   @Before
   public void setUp() {
      this.data = new byte[1024];
   }

   /**
    * Writes and reads a byte.
    */
   @Test
   public void testByte() {
      final byte min = Byte.MIN_VALUE;
      final byte max = Byte.MAX_VALUE;

      Convert.writeByte(this.data, 0, min);
      Convert.writeByte(this.data, 8, max);

      assertEquals(min, Convert.readByte(this.data, 0));
      assertEquals(max, Convert.readByte(this.data, 8));
   }

   /**
    * Writes and reads a char.
    */
   @Test
   public void testChar() {
      final char min = Character.MIN_VALUE;
      final char max = Character.MAX_VALUE;

      Convert.writeChar(this.data, 0, min);
      Convert.writeChar(this.data, 8, max);

      assertEquals(min, Convert.readChar(this.data, 0));
      assertEquals(max, Convert.readChar(this.data, 8));
   }

   /**
    * Writes and reads a short.
    */
   @Test
   public void testShort() {
      final short min = Short.MIN_VALUE;
      final short max = Short.MAX_VALUE;

      Convert.writeShort(this.data, 0, min);
      Convert.writeShort(this.data, 8, max);

      assertEquals(min, Convert.readShort(this.data, 0));
      assertEquals(max, Convert.readShort(this.data, 8));
   }

   /**
    * Writes and reads an integer.
    */
   @Test
   public void testInt() {
      final int min = Integer.MIN_VALUE;
      final int max = Integer.MAX_VALUE;

      Convert.writeInt(this.data, 0, min);
      Convert.writeInt(this.data, 8, max);

      assertEquals(min, Convert.readInt(this.data, 0));
      assertEquals(max, Convert.readInt(this.data, 8));
   }

   /**
    * Writes and reads a long.
    */
   @Test
   public void testLong() {
      final long min = Long.MIN_VALUE;
      final long max = Long.MAX_VALUE;

      Convert.writeLong(this.data, 0, min);
      Convert.writeLong(this.data, 8, max);

      assertEquals(min, Convert.readLong(this.data, 0));
      assertEquals(max, Convert.readLong(this.data, 8));
   }

   /**
    * Writes and reads a float.
    */
   @Test
   public void testFloat() {
      final float min = Float.MIN_VALUE;
      final float max = Float.MAX_VALUE;

      Convert.writeFloat(this.data, 0, min);
      Convert.writeFloat(this.data, 8, max);

      assertEquals(min, Convert.readFloat(this.data, 0), 0);
      assertEquals(max, Convert.readFloat(this.data, 8), 0);
   }

   /**
    * Writes and reads a double.
    */
   @Test
   public void testDouble() {
      final double min = Double.MIN_VALUE;
      final double max = Double.MAX_VALUE;

      Convert.writeDouble(this.data, 0, min);
      Convert.writeDouble(this.data, 8, max);

      assertEquals(min, Convert.readDouble(this.data, 0), 0);
      assertEquals(max, Convert.readDouble(this.data, 8), 0);
   }

   /**
    * Writes and reads a string.
    */
   @Test
   public void testString() {
      final String strTest = "foobar";
      final String strEmpty = "";
      final String strSpecial = "\\ \n \t ' \"";

      Convert.writeString(this.data, 0, strTest, strTest.length());
      assertEquals(strTest, Convert.readString(this.data, 0, strTest.length()));

      Convert.writeString(this.data, 0, strEmpty, strEmpty.length());
      assertEquals(strEmpty, Convert.readString(this.data, 0, strEmpty.length()));

      Convert.writeString(this.data, 0, strSpecial, strSpecial.length());
      assertEquals(strSpecial, Convert.readString(this.data, 0, strSpecial.length()));

      Convert.writeString(this.data, 0, strTest, strTest.length() - 1);
      assertEquals(strTest.substring(0, strTest.length() - 1), Convert
            .readString(this.data, 0, strTest.length() - 1));
   }

   /**
    * Writes and reads the latest Date possible.
    * Later dates can be written into the byte-array but java.sql.Date cannot represent them.
    */
   @Test
   public void testDateMax() {
      final Date date = Date.valueOf(LocalDate.of(9999, 12, 31));
      Convert.writeDate(this.data, 0, date);
      final Date dateRead = Convert.readDate(this.data, 0);

      assertEquals(date, dateRead);
   }

   /**
    * Writes and reads the earliest Date possible.
    * Earlier dates could be written into the byte-array but java.sql.Date cannot represent them.
    */
   @Test
   public void testDateMin() {
      final Date date = Date.valueOf(LocalDate.of(1, 1, 1));
      Convert.writeDate(this.data, 0, date);
      final Date dateRead = Convert.readDate(this.data, 0);

      assertEquals(date, dateRead);
   }

   /**
    * Writes and reads the earliest time possible.
    */
   @Test
   public void testTimeMin() {
      final Time time = Time.valueOf(LocalTime.MIN);
      Convert.writeTime(this.data, 0, time);
      final Time timeRead = Convert.readTime(this.data, 0);

      assertEquals(time, timeRead);
   }

   /**
    * Writes and reads the latest time possible.
    */
   @Test
   public void testTimeMax() {
      final Time time = Time.valueOf(LocalTime.MAX);
      Convert.writeTime(this.data, 0, time);
      final Time timeRead = Convert.readTime(this.data, 0);

      assertEquals(time, timeRead);
   }

   /**
    * Writes and reads the earliest timestamp possible.
    */
   @Test
   public void testTimestampMin() {
      final Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(LocalDate.of(1, 1, 1), LocalTime.of(23, 59, 59)));

      Convert.writeTimestamp(this.data, 0, timestamp);
      final Timestamp timestampRead = Convert.readTimestamp(this.data, 0);

      assertEquals(timestamp, timestampRead);
   }

   /**
    * Writes and reads the latest timestamp possible.
    */
   @Test
   public void testTimestampMax() {
      final Timestamp timestamp = Timestamp
            .valueOf(LocalDateTime.of(LocalDate.of(9999, 12, 31), LocalTime.of(23, 59, 59)));
      Convert.writeTimestamp(this.data, 0, timestamp);
      final Timestamp timestampRead = Convert.readTimestamp(this.data, 0);

      assertEquals(timestamp, timestampRead);
   }
}

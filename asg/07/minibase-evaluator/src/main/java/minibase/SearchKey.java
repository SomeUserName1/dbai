/*
 * @(#)SearchKey.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;


import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageType;

/**
 * Provides a general and type-safe way to store and compare index search keys.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public class SearchKey implements Comparable<SearchKey> {

   // --------------------------------------------------------------------------

   /** The type of the key value. */
   private final SearchKeyType type;

   /** The size of the key value (in bytes). */
   private final short size;

   /** The actual key value. */
   private final Object[] values;

   // --------------------------------------------------------------------------

   /**
    * Constructs a new long search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Long value) {
      this.type = AtomicKeyType.LONG;
      this.size = Long.BYTES;
      this.values = new Long[]{value};
   }

   /**
    * Constructs a new integer search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Integer value) {
      this.type = AtomicKeyType.INTEGER;
      this.size = Integer.BYTES;
      this.values = new Integer[]{value};
   }

   /**
    * Constructs a new short search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Short value) {
      this.type = AtomicKeyType.SHORT;
      this.size = Short.BYTES;
      this.values = new Short[]{value};
   }

   /**
    * Constructs a new byte search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Byte value) {
      this.type = AtomicKeyType.BYTE;
      this.size = Byte.BYTES;
      this.values = new Byte[]{value};
   }

   /**
    * Constructs a new float search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Float value) {
      this.type = AtomicKeyType.FLOAT;
      this.size = Float.BYTES;
      this.values = new Float[]{value};
   }

   /**
    * Constructs a new float search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Double value) {
      this.type = AtomicKeyType.DOUBLE;
      this.size = Double.BYTES;
      this.values = new Double[]{value};
   }

   /**
    * Constructs a new date search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Timestamp value) {
      this.type = AtomicKeyType.TIMESTAMP;
      this.size = Long.BYTES;
      this.values = new Timestamp[]{value};
   }

   /**
    * Constructs a new date search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Date value) {
      this.type = AtomicKeyType.DATE;
      this.size = Long.BYTES;
      this.values = new Date[]{value};
   }

   /**
    * Constructs a new date search key.
    *
    * @param value
    *           the search key
    */
   public SearchKey(final Time value) {
      this.type = AtomicKeyType.TIME;
      this.size = Long.BYTES;
      this.values = new Time[]{value};
   }

   /**
    * Constructs a new string search key.
    * @param type
    *          the type of the key
    * @param value
    *           the search key
    */
   public SearchKey(final AtomicKeyType.StringKeyType type, final String value) {
      this.type = type;
      this.size = (short) type.getKeyLength();
      this.values = new String[]{value};
   }

   /**
    * Constructs a new key of the given type.
    * @param type the type of the key
    * @param values the values of the key
    */
   public SearchKey(final SearchKeyType type, final Object...values) {
      this.type = type;
      this.size = (short) type.getKeyLength();
      this.values = values;
   }

   /**
    * Writes the SearchKey's value into the given data buffer.
    *
    * @param data
    *           the byte array to write to
    * @param offset
    *           write offset
    */
   public void writeRawData(final byte[] data, final int offset) {
      this.type.writeRawData(data, offset, this.values);
   }

   /**
    * Gets the total length of the search key (in bytes).
    *
    * @return number of bytes this search key occupies
    */
   public short getLength() {
      return this.size;
   }

   /**
    * @return the type of the search key
    */
   public SearchKeyType getType() {
      return this.type;
   }

   /**
    * Gets the key stored at the given position.
    * @param page the page
    * @param keyType type of the key as byte
    * @param offset the offset of the wanted key
    * @return the key's value
    */
   public static SearchKey getKey(final Page<? extends PageType> page,
         final SearchKeyType keyType, final int offset) {
      return keyType.readSearchKey(page.getData(), offset);
   }

   // --------------------------------------------------------------------------

   @Override
   public int hashCode() {
      return this.type.getHashCode(this.values, 0);
   }

   @Override
   public boolean equals(final Object obj) {
      if (!(obj instanceof SearchKey)) {
         return false;
      }
      final SearchKey key = (SearchKey) obj;
      if (this.values.length != key.values.length) {
         return false;
      }
      for (int i = 0; i < this.values.length; i++) {
         if (!this.values[i].equals(key.values[i])) {
            return false;
         }
      }
      return true;
   }

   @Override
   public int compareTo(final SearchKey key) {
      if (!this.type.equals(key.type)) {
         throw new IllegalArgumentException("Search keys are not comparable: " + this.type + " <-> " + key.type);
      }
      return this.type.compareValues(this.values, key.values);
   }

   @Override
   public String toString() {
      return "SearchKey{" + this.type.getString(this.values, 0) + "}";
   }
}

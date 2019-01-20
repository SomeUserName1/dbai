/*
 * @(#)SearchKeyType.java   1.0   Mar 24, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import minibase.catalog.DataType;
import minibase.util.Convert;

/**
 * The Superclass for the different key types in the indexes.
 *
 * @author Raffael Wagner
 * @version 1.0
 */
public abstract class SearchKeyType {

   /**
    * The different kinds of search key types.
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   public enum Kind {
      /** The Composite key. */
      COMPOSITE,
      /** The Long key. */
      LONG,
      /** The Integer key. */
      INTEGER,
      /** The Short key. */
      SHORT,
      /** The Byte key. */
      BYTE,
      /** The Float key. */
      FLOAT,
      /** The Double key. */
      DOUBLE,
      /** The Datetime key. */
      TIMESTAMP,
      /** The Date key. */
      DATE,
      /** The Time key. */
      TIME,
      /** The String key. */
      STRING
   }

   /** The size of the key. */
   private final int keyLength;

   /** The length of the type. */
   private final int length;

   /** The kind of the key. */
   private final Kind kind;

   /**
    * Create a new KeyType with the given size.
    * @param kind the kind of the key
    * @param length the length of the type
    * @param keyLength the size of the key
    */
   SearchKeyType(final Kind kind, final int length, final int keyLength) {
      this.length = length;
      this.keyLength = keyLength;
      this.kind = kind;
   }

   /**
    * @return the size of the key
    */
   public int getKeyLength() {
      return this.keyLength;
   }

   /**
    * @return the length of the type
    */
   public int getLength() {
      return this.length;
   }

   /**
    * @return the id of the key class
    */
   public final byte getID() {
      return (byte) this.kind.ordinal();
   }

   /**
    * Read the key type from the data and return the instance.
    * @param data the data of the index
    * @param offset the offset in the data where the type is stored
    * @return the found KeyType
    */
   public static SearchKeyType readFrom(final byte[] data, final int offset) {
      final byte typeID = data[offset];
      switch (Kind.values()[typeID]) {
         case LONG:
            return AtomicKeyType.LONG;
         case INTEGER:
            return AtomicKeyType.INTEGER;
         case SHORT:
            return AtomicKeyType.SHORT;
         case BYTE:
            return AtomicKeyType.BYTE;
         case FLOAT:
            return AtomicKeyType.FLOAT;
         case DOUBLE:
            return AtomicKeyType.DOUBLE;
         case TIMESTAMP:
            return AtomicKeyType.TIMESTAMP;
         case DATE:
            return AtomicKeyType.DATE;
         case TIME:
            return AtomicKeyType.TIME;
         case STRING:
            return AtomicKeyType.StringKeyType.getInstance(Convert.readShort(data, offset + 1));
         case COMPOSITE:
            return CompositeKeyType.read(data, offset);
         default:
            throw new IllegalArgumentException("Unknown search key type: " + typeID);
      }
   }

   /**
    * Gets the search key type of a data type.
    * @param type data type
    * @param length field length
    * @return search key type
    */
   public static AtomicKeyType get(final DataType type, final int length) {
      switch (type) {
         case BIGINT:
            return AtomicKeyType.LONG;
         case INT:
            return AtomicKeyType.INTEGER;
         case SMALLINT:
            return AtomicKeyType.SHORT;
         case TINYINT:
            return AtomicKeyType.BYTE;
         case FLOAT:
            return AtomicKeyType.FLOAT;
         case DOUBLE:
            return AtomicKeyType.DOUBLE;
         case DATETIME:
            return AtomicKeyType.TIMESTAMP;
         case DATE:
            return AtomicKeyType.DATE;
         case TIME:
            return AtomicKeyType.TIME;
         case VARCHAR:
         case CHAR:
            return AtomicKeyType.StringKeyType.getInstance(length);
         default:
            throw new IllegalArgumentException("Unknown data type: " + type);
      }
   }

   /**
    * Read the value of the key.
    * @param data the data of the index
    * @param offset the offset in the data where the type is stored
    * @param keySize
    * @return the value of the key
    */
   public abstract SearchKey readSearchKey(byte[] data, int offset);

   /**
    * Write the values to the page.
    * @param data the data to be written to
    * @param offset the position to start writing
    * @param values the values to be written
    */
   public final void writeRawData(final byte[] data, final int offset, final Object[] values) {
      this.writeRawData(data, offset, values, 0);
   }

   /**
    * Write the values to the page.
    * @param data the data to be written to
    * @param offset the position to start writing
    * @param values the values to be written
    * @param index the index of the values that should be written
    */
   public abstract void writeRawData(byte[] data, int offset, Object[] values, int index);

   /**
    * @param values the values to get the hashcode from
    * @param index the index to look at
    * @return the hash code
    */
   public abstract int getHashCode(Object[] values, int index);

   /**
    * Compares two arrays of this type by comparing each entry.
    * @param values1 the first value
    * @param values2 the second value
    * @return the result
    */
   public int compareValues(final Object[] values1, final Object[] values2) {
      return this.compareValues(values1, values2, 0);
   }

   /**
    * Compares two arrays of this type by comparing each entry.
    * @param values1 the first value
    * @param values2 the second value
    * @param index the index to look at
    * @return the result
    */
   public abstract int compareValues(Object[] values1, Object[] values2, int index);

   /**
    * @param values the values of the key
    * @param index the index to look at
    * @return the values as string
    */
   public abstract String getString(Object[] values, int index);

   /**
    * Write the type to the given data.
    * @param data the data to be written to
    * @param offset the position to start writing at
    */
   public abstract void writeTo(byte[] data, int offset);
}

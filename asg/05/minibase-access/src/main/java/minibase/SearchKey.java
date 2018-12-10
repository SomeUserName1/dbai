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

import minibase.util.Convert;

/**
 * Provides a general and type-safe way to store and compare index search keys.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public class SearchKey implements Comparable<SearchKey> {

   /** Types of supported search keys. */
   enum KeyType {
      /** 32-bit integer key. */
      INTEGER((byte) 1),
      /** 32-bit floating point key. */
      FLOAT((byte) 2),
      /** String key. */
      STRING((byte) 3);

      /** The key type's internal ID. */
      private final byte id;

      /**
       * Constructor for key types.
       * 
       * @param id
       *           the type's internal ID
       */
      KeyType(final byte id) {
         this.id = id;
      }

      /**
       * Gets the key type with the given ID.
       * 
       * @param id
       *           the key type ID
       * @return the corresponding key type
       * @throws IllegalArgumentException
       *            if the ID is not known
       */
      public static KeyType getInstance(final byte id) {
         for (final KeyType key : KeyType.values()) {
            if (key.id == id) {
               return key;
            }
         }
         throw new IllegalArgumentException("Unknown search key type: " + id);
      }

      /**
       * Returns this key type's internal ID.
       * 
       * @return the ID
       */
      public byte getID() {
         return this.id;
      }
   }

   // --------------------------------------------------------------------------

   /** The type of the key value. */
   private final KeyType type;

   /** The size of the key value (in bytes). */
   private final short size;

   /** The actual key value. */
   private final Object value;

   // --------------------------------------------------------------------------

   /**
    * Constructs a new integer search key.
    * 
    * @param value
    *           the search key
    */
   public SearchKey(final Integer value) {
      this.type = KeyType.INTEGER;
      this.size = Integer.SIZE / Byte.SIZE;
      this.value = value;
   }

   /**
    * Constructs a new float search key.
    * 
    * @param value
    *           the search key
    */
   public SearchKey(final Float value) {
      this.type = KeyType.FLOAT;
      this.size = Float.SIZE / Byte.SIZE;
      this.value = value;
   }

   /**
    * Constructs a new string search key.
    * 
    * @param value
    *           the search key
    */
   public SearchKey(final String value) {
      this.type = KeyType.STRING;
      this.size = (short) value.length();
      this.value = value;
   }

   /**
    * Constructs a search key from a generic value.
    *
    * @param value
    *           the search key
    * @throws IllegalArgumentException
    *            if value's type is invalid
    */
   public SearchKey(final Object value) {

      if (value instanceof Integer) {
         // Integer key?
         this.type = KeyType.INTEGER;
         this.size = Integer.SIZE / Byte.SIZE;
         this.value = value;
      } else if (value instanceof Float) {
         // Float key?
         this.type = KeyType.FLOAT;
         this.size = Float.SIZE / Byte.SIZE;
         this.value = value;
      } else if (value instanceof String) {
         // String key?
         this.type = KeyType.STRING;
         this.size = (short) ((String) value).length();
         this.value = value;
      } else {
         // otherwise, none of the above
         throw new IllegalArgumentException("invalid key value type");
      }
   }

   /**
    * Constructs a SearchKey stored in the given data buffer.
    * 
    * @param data
    *           byte array to read from
    * @param offset
    *           read offset
    */
   public SearchKey(final byte[] data, final int offset) {

      // extract the type and size
      this.type = KeyType.getInstance(data[offset]);
      this.size = Convert.readShort(data, offset + 1);

      // extract the key value
      switch (this.type) {
         case INTEGER:
            this.value = Convert.readInt(data, offset + 3);
            break;
         case FLOAT:
            this.value = Convert.readFloat(data, offset + 3);
            break;
         case STRING:
            this.value = Convert.readString(data, offset + 3, this.size);
            break;
         default:
            throw new IllegalArgumentException("Key type '" + this.type + "' not recognized.");
      }
   }

   /**
    * Writes the SearchKey into the given data buffer.
    * 
    * @param data
    *           the byte array to write to
    * @param offset
    *           write offset
    */
   public void writeData(final byte[] data, final int offset) {

      // write the type and size
      data[offset] = this.type.getID();
      Convert.writeShort(data, offset + 1, this.size);

      // write the key value
      switch (this.type) {
         case INTEGER:
            Convert.writeInt(data, offset + 3, (Integer) this.value);
            break;
         case FLOAT:
            Convert.writeFloat(data, offset + 3, (Float) this.value);
            break;
         case STRING:
            Convert.writeString(data, offset + 3, (String) this.value, this.size);
            break;
         default:
            throw new IllegalArgumentException("Key type '" + this.type + "' not recognized.");
      }
   }

   /**
    * Gets the total length of the search key (in bytes).
    * 
    * @return number of bytes this search key occupies
    */
   public short getLength() {
      return (short) (3 + this.size);
   }

   /**
    * Reads the total length of the search key (in bytes) from the given data buffer at the specified offset.
    * This method is equivalent with {@code (new SearchKey(data, offset)).getLength()}.
    *
    * @param data
    *           byte buffer to read from
    * @param offset
    *           read offset
    * @return number of bytes the search key that could have been instantiated from the data at the specified
    *         position would occupy
    */
   public static short readLength(final byte[] data, final int offset) {
      return (short) (3 + Convert.readShort(data, offset + 1));
   }

   // --------------------------------------------------------------------------

   /**
    * Gets the hash value for the search key, given the depth (i.e. number of bits to consider).
    * 
    * @param depth
    *           number of bits to consider
    * @return the hash value
    */
   public int getHash(final int depth) {
      // apply the appropriate calculation
      final int mask = (1 << depth) - 1;
      switch (this.type) {
         case INTEGER:
            return (Integer) this.value & mask;
         case FLOAT:
            return Float.floatToIntBits((Float) this.value) & mask;
         case STRING:
         default:
            return this.value.hashCode() & mask;
      }
   }

   /**
    * Returns true if the search key matches the given hash value, false otherwise.
    * 
    * @param hash
    *           the hash value to compare to
    * @return {@code true} if this key matches the given hash, {@code false} otherwise
    */
   public boolean isHash(final int hash) {
      // calculate the bit depth (i.e. the left-most '1' bit)
      final int depth = (int) (Math.log(hash) / Math.log(2) + 1);

      // compare the hash codes
      return this.getHash(depth) == hash;
   }

   // --------------------------------------------------------------------------

   @Override
   public int hashCode() {
      return this.value.hashCode();
   }

   @Override
   public boolean equals(final Object obj) {
      return obj instanceof SearchKey && ((SearchKey) obj).value.equals(this.value);
   }

   @Override
   public int compareTo(final SearchKey key) {
      if (this.type != key.type) {
         throw new IllegalArgumentException("Search keys are not comparable: " + this.type + " <-> " + key.type);
      }

      switch (this.type) {
         case INTEGER:
            return ((Integer) this.value).compareTo((Integer) key.value);
         case FLOAT:
            return ((Float) this.value).compareTo((Float) key.value);
         case STRING:
         default:
            return ((String) this.value).compareTo((String) key.value);
      }
   }

   @Override
   public String toString() {
      return "SearchKey{" + this.value.toString() + "}";
   }
}

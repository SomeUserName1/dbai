/*
 * @(#)CompositeKeyType.java   1.0   Apr 5, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.util.Arrays;

/**
 * The class for the composite key type.
 *
 * @author Raffael Wagner
 * @version 1.0
 */
public final class CompositeKeyType extends SearchKeyType {

   /** The types that this key consists of. **/
   private AtomicKeyType[] types;

   /**
    * Creates a Composite Key Type.
    * @param types The types of the key
    */
   private CompositeKeyType(final AtomicKeyType[] types) {
      super(Kind.COMPOSITE, length(types), keyLength(types));
      this.types = types;
   }

   /**
    * @param types The types of the single keys.
    * @return an IntegerKeyType instance
    */
   public static CompositeKeyType getInstance(final AtomicKeyType[] types) {
      return new CompositeKeyType(types);
   }

   /**
    * @param types The types of the single keys
    * @return the combined length of the types
    */
   private static int length(final AtomicKeyType[] types) {
      int length = 2;
      for (AtomicKeyType type : types) {
         length += type.getLength();
      }
      return length;
   }

   /**
    * @param types The types of the single keys
    * @return the combined length of the keys
    */
   private static int keyLength(final AtomicKeyType[] types) {
      int keyLength = 0;
      for (AtomicKeyType type : types) {
         keyLength += type.getKeyLength();
      }
      return keyLength;
   }

   /**
    * Returns a Composite Key Type instance.
    * @param data the data that the key is read from
    * @param offset the offset to start the read
    * @return A Composite Key Type instance
    */
   public static CompositeKeyType read(final byte[] data, final int offset) {
      int off = offset + 1;
      final int amountKeys = data[off++];
      final AtomicKeyType[] types = new AtomicKeyType[amountKeys];
      for (int i = 0; i < amountKeys; i++) {
         types[i] = (AtomicKeyType) SearchKeyType.readFrom(data, off);
         off += types[i].getLength();
      }
      return new CompositeKeyType(types);
   }

   /**
    * @return the types of the key
    */
   public SearchKeyType[] getTypes() {
      return this.types;
   }

   @Override
   public SearchKey readSearchKey(final byte[] data, final int offset) {
      final Object[] values = new Object[this.types.length];
      int curOffset = offset;
      for (int i = 0; i < this.types.length; i++) {
         final AtomicKeyType type = this.types[i];
         values[i] = type.readObject(data, curOffset);
         curOffset += type.getKeyLength();
      }
      return new SearchKey(this, values);
   }

   @Override
   public void writeRawData(final byte[] data, final int offset, final Object[] values, final int index) {
      int curOffset = offset;
      for (int i = 0; i < values.length; i++) {
         final AtomicKeyType type = this.types[i];
         type.writeRawData(data, curOffset, values, i);
         curOffset += type.getKeyLength();
      }
   }

   @Override
   public int getHashCode(final Object[] values, final int index) {
      int hash = 0;
      for (int i = 0; i < values.length; i++) {
         final AtomicKeyType type = this.types[i];
         hash = 31 * hash + type.getHashCode(values, i);
      }
      return hash;
   }

   @Override
   public int compareValues(final Object[] values1, final Object[] values2, final int index) {
      for (int i = 0; i < values1.length; i++) {
         final AtomicKeyType type = this.types[i];
         final int compareValues = type.compareValues(values1, values2, i);
         if (compareValues != 0) {
            return compareValues;
         }
      }
      return 0;
   }

   @Override
   public String getString(final Object[] values, final int index) {
      final StringBuilder s = new StringBuilder();
      for (int i = 0; i < values.length; i++) {
         final AtomicKeyType type = this.types[i];
         s.append(type.getString(values, i));
         if (i < values.length - 1) {
            s.append(", ");
         }
      }
      return s.toString();
   }

   @Override
   public void writeTo(final byte[] data, final int offset) {
      int off = offset;
      data[off++] = this.getID();
      data[off++] = (byte) this.types.length;
      for (int i = 0; i < this.types.length; i++) {
         this.types[i].writeTo(data, off);
         off += this.types[i].getLength();
      }
   }

   @Override
   public int hashCode() {
      int hash = this.getID();
      for (final AtomicKeyType type : this.types) {
         hash = 31 * hash + type.hashCode();
      }
      return hash;
   }

   @Override
   public boolean equals(final Object obj) {
      if (obj == this) {
         return true;
      }
      if (obj == null || this.getClass() != obj.getClass()) {
         return false;
      }
      final CompositeKeyType other = (CompositeKeyType) obj;
      return Arrays.equals(this.types, other.types);
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(this.getClass().getSimpleName()).append('[');
      for (int i = 0; i < this.types.length; i++) {
         if (i > 0) {
            sb.append(", ");
         }
         this.types[i].toString(sb);
      }
      return sb.append(']').toString();
   }
}

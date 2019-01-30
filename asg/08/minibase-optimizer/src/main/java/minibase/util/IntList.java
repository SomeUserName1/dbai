/*
 * @(#)IntList.java   1.0   Jun 13, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.util;

/**
 * An efficient implementation of {@code ArrayList<Integer>} working on primitive {@code int} arrays.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class IntList {
   /** Values in this list. */
   private int[] values;
   /** Logical size of this list. */
   private int size;

   /**
    * Creates a new, empty list with initial capacity {@code 4}.
    */
   public IntList() {
      this(4);
   }

   /**
    * Creates a new, empty list with the given initial capacity.
    *
    * @param initialCapacity initial capacity, ignored if smaller than one
    */
   public IntList(final int initialCapacity) {
      this.values = new int[Math.max(initialCapacity, 1)];
      this.size = 0;
   }

   /**
    * Returns the element at the given position in this list.
    *
    * @param pos position of the entry
    * @return value of the entry
    */
   public int get(final int pos) {
      return this.values[pos];
   }

   /**
    * Adds an element to this list.
    *
    * @param value element to add
    */
   public void add(final int value) {
      int[] values = this.values;
      final int len = values.length;
      if (this.size == len) {
         values = new int[2 * len];
         System.arraycopy(this.values, 0, values, 0, len);
         this.values = values;
      }
      values[this.size++] = value;
   }

   /**
    * Returns the current size of this list.
    *
    * @return number of elements in this list
    */
   public int size() {
      return this.size;
   }

   /**
    * Returns an {@code int} array containing the values in this list.
    *
    * @return values of this list
    */
   public int[] toArray() {
      final int[] result = new int[this.size];
      System.arraycopy(this.values, 0, result, 0, result.length);
      return result;
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof IntList)) {
         return false;
      }
      final IntList that = (IntList) obj;
      if (this.size != that.size) {
         return false;
      }
      for (int i = 0; i < this.size; i++) {
         if (this.values[i] != that.values[i]) {
            return false;
         }
      }
      return true;
   }

   @Override
   public int hashCode() {
      int hash = this.size;
      for (int i = 0; i < this.size; i++) {
         hash = 31 * hash + this.values[i];
      }
      return hash;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName()).append('[');
      final int n = this.size;
      if (n > 0) {
         sb.append(this.values[0]);
         for (int i = 1; i < n; i++) {
            sb.append(", ").append(this.values[i]);
         }
      }
      return sb.append(']').toString();
   }
}

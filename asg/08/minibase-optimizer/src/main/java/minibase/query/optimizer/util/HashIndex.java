/*
 * @(#)HashIndex.java   1.0   May 16, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.util;

import java.util.function.BiFunction;

/**
 * Simple hash-based in-memory index structure that is used by the search space.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 * @param <E>
 *           type of index entries
 */
@SuppressWarnings("unchecked")
public class HashIndex<E> {

   /** Table of index entries. */
   private final IndexEntry<E>[] table;

   /**
    * Creates a new hash-based in-memory index structure of the given capacity. If the capacity is reached or
    * if there are hash collisions, overflow chains will be formed.
    *
    * @param capacity
    *           index capacity
    */
   public HashIndex(final int capacity) {
      this.table = new IndexEntry[capacity];
   }

   /**
    * Inserts the given key-value pair into the index, if it is not already present. The method returns
    * {@code true} if the key-value pair was inserted and {@code false} if it was already present.
    *
    * @param key
    *           index key
    * @param value
    *           value to index
    * @return {@code true} if the key-value pair was inserted, {@code false} otherwise
    */
   public boolean insert(final int key, final E value) {
      final int index = (key & 0x7FFFFFFF) % this.table.length;
      for (IndexEntry<E> entry = this.table[index]; entry != null; entry = entry.next) {
         if (entry.value.equals(value)) {
            return false;
         }
      }
      final IndexEntry<E> entry = this.table[index];
      this.table[index] = new IndexEntry<>(value, entry);
      return true;
   }

   /**
    * Checks if the given key-value pair is present in the index and returns the value from the index if it
    * is. Otherwise, the method returns {@code null}.
    *
    * @param key
    *           index key
    * @param value
    *           value to check
    * @return value from index
    */
   public E contains(final int key, final E value) {
      return this.contains(key, value, (final E object, final E other) -> object.equals(other));
   }

   /**
    * Checks if the given key-value pair is present in the index and returns the value from the index if it
    * is. Otherwise, the method returns {@code null}.
    *
    * @param key
    *           index key
    * @param value
    *           value to check
    * @param equalsFunction
    *           function to be used for equality check
    * @return value from index
    */
   public E contains(final int key, final E value, final BiFunction<E, E, Boolean> equalsFunction) {
      final int index = (key & 0x7FFFFFFF) % this.table.length;
      for (IndexEntry<E> entry = this.table[index]; entry != null; entry = entry.next) {
         if (equalsFunction.apply(entry.value, value).booleanValue()) {
            return entry.value;
         }
      }
      return null;
   }

   /**
    * Wraps an index entry to support overflow chains.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    * @param <T>
    *           type of index entries
    */
   private static final class IndexEntry<T> {

      /** Value to index. */
      private final T value;

      /** Next entry in overflow chain. */
      private final IndexEntry<T> next;

      /**
       * Constructs a new index entry for the provided value.
       *
       * @param value
       *           value to index
       * @param next
       *           next entry in overflow chain
       */
      private IndexEntry(final T value, final IndexEntry<T> next) {
         this.value = value;
         this.next = next;
      }
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("HashIndex{");
      for (int i = 0; i < this.table.length; i++) {
         final IndexEntry<E> entry = this.table[i];
         if (entry == null) {
            continue;
         }
         if (sb.length() > 10) {
            sb.append(", ");
         }
         sb.append(String.format("%04x: [", i));
         boolean start = true;
         for (IndexEntry<E> curr = entry; curr != null; curr = curr.next) {
            if (!start) {
               sb.append(", ");
            }
            sb.append(curr.value);
            start = false;
         }
         sb.append("]");
      }
      return sb.append("}").toString();
   }
}

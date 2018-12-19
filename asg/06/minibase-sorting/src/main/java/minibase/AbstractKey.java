/*
 * @(#)AbstractKey.java   1.0   Mar 4, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract implementation of a Minibase key.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @param <T>
 *           generic type of the columns in this key
 */
public abstract class AbstractKey<T> implements Key<T> {

   /** List of columns in this key. */
   private final List<T> columns;

   /**
    * Constructs a new key with the given list of columns.
    *
    * @param columns
    *           key column list
    */
   protected AbstractKey(final List<T> columns) {
      this.columns = columns;
   }

   @Override
   public T getColumn(final int index) {
      return this.columns.get(index);
   }

   @Override
   public boolean contains(final T column) {
      return this.columns.contains(column);
   }

   @Override
   public int size() {
      return this.columns.size();
   }

   @Override
   public Iterator<T> iterator() {
      return this.columns.iterator();
   }

   @Override
   public boolean isSubsetOf(final Collection<T> columns) {
      if (columns.size() < this.size()) {
         return false;
      }
      for (final T id : this) {
         if (!columns.contains(id)) {
            return false;
         }
      }
      return true;
   }

   @Override
   public Stream<T> stream() {
      return this.columns.stream();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (this.columns == null ? 0 : this.columns.hashCode());
      return result;
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (this.getClass() != obj.getClass()) {
         return false;
      }
      final AbstractKey< ? > other = (AbstractKey< ? >) obj;
      if (this.columns == null) {
         if (other.columns != null) {
            return false;
         }
      } else if (this.columns.size() != other.columns.size()) {
         return false;
      } else if (!this.equals(this.columns, other.columns)) {
         return false;
      }
      return true;
   }

   /**
    * Compare the two column lists.
    *
    * @param columns
    *           columns of this key
    * @param other
    *           columns of the other key
    * @return {@code true} if the two lists are equal, {@code false} otherwise
    */
   public abstract boolean equals(List< ? > columns, List< ? > other);

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append("(");
      result.append(this.columns.stream().map(i -> i + "").collect(Collectors.joining(", ")));
      result.append(")");
      return result.toString();
   }
}

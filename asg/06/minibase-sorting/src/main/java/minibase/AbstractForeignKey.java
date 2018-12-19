/*
 * @(#)AbstractForeignKey.java   1.0   Mar 4, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

/**
 * Abstract implementation of a Minibase key.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @param <T>
 *           generic type of the columns in this key
 */

public abstract class AbstractForeignKey<T extends Key< ? >> implements ForeignKey<T> {

   /** Key containing referencing columns. */
   private T referencing;

   /** Key containing referenced columns. */
   private T referenced;

   /**
    * Constructs a new foreign key with the given key columns and the given referenced columns.
    *
    * @param referencing
    *           referencing key
    * @param referenced
    *           referenced key
    */
   protected AbstractForeignKey(final T referencing, final T referenced) {
      this.referencing = referencing;
      this.referenced = referenced;
   }

   /**
    * Sets the referencing key columns of this foreign key.
    *
    * @param referencing
    *           referencing key
    */
   protected void setReferencingColumns(final T referencing) {
      this.referencing = referencing;
   }

   @Override
   public T getReferencingColumns() {
      return this.referencing;
   }

   /**
    * Sets the referenced key columns of this foreign key.
    *
    * @param referenced
    *           referenced key
    */
   protected void setReferencedColumns(final T referenced) {
      this.referenced = referenced;
   }

   @Override
   public T getReferencedColumns() {
      return this.referenced;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append(this.referencing);
      result.append("->");
      result.append(this.referenced);
      return result.toString();
   }
}

/*
 * @(#)StrongReference.java   1.0   May 10, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.util;

/**
 * Utility class to emulate in/out method parameters in Java.
 * <p>
 * Minibase's query optimizer is based on the Cascades framework for query optimization and, additionally,
 * implements some of the improvements proposed by the Columbia database query optimizer.
 * <ul>
 * <li>Goetz Graefe: <strong>The Cascades Framework for Query Optimization</strong>. In
 * <em>IEEE Data(base) Engineering Bulletin</em>, 18(3), pp. 19-29, 1995.</li>
 * <li>Yongwen Xu: <strong>Efficiency in Columbia Database Query Optimizer</strong>,
 * <em>MSc Thesis, Portland State University</em>, 1998.</li>
 * </ul>
 * The Minibase query optimizer therefore descends from the EXODUS, Volcano, Cascades, and Columbia line of
 * query optimizers, which all use a rule-based, top-down approach to explore the space of possible query
 * execution plans, rather than a bottom-up approach based on dynamic programming.
 * </p>
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 * @param <T>
 *           template type of the referenced object
 */
public final class StrongReference<T> {

   /** Referenced object. */
   private T value;

   /**
    * Creates a new reference to the given object.
    *
    * @param value
    *           referenced object
    */
   public StrongReference(final T value) {
      this.value = value;
   }

   /**
    * Creates a new reference that points to {@code null}.
    */
   public StrongReference() {
      this(null);
   }

   /**
    * Returns the object pointed to by this reference.
    *
    * @return referenced object
    */
   public T get() {
      return this.value;
   }

   /**
    * Sets the object pointed to by this reference.
    *
    * @param value
    *           referenced object
    */
   public void set(final T value) {
      this.value = value;
   }

   /**
    * Returns whether or not this is a {@code null} reference.
    *
    * @return {@code true} if this is a {@code null} reference, {@code false} otherwise
    */
   public boolean isNull() {
      return this.value == null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int hashCode() {
      return this.value.hashCode();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean equals(final Object other) {
      if (StrongReference.class.equals(other.getClass())) {
         return this.value.equals(((StrongReference< ? >) other).value);
      }
      return false;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      return String.valueOf(this.value);
   }
}

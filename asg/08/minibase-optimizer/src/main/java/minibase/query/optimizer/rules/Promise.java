/*
 * @(#)Promise.java   1.0   May 9, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

/**
 * Enumeration to describe the different promise values that rules can have.
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
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public enum Promise {

   /** Promise of applying a rule that inserts a file scan. */
   FILESCAN(5),
   /** Promise of applying a rule that inserts a sort operator. */
   SORT(6),
   /** Promise of applying a rule that inserts a merge operator. */
   MERGE(4),
   /** Promise of applying a rule that inserts a hash operator. */
   HASH(4),
   /** Promise of applying a physical rule. */
   PHYSCIAL(3),
   /** Promise of applying a logical rule. */
   LOGICAL(1),
   /** Promise of applying an associative rule. */
   ASSOCIATIVE(2),
   /** Promise to avoid a rule from firing. */
   NONE(0);

   /** Promise value. */
   private final int value;

   /**
    * Constructs a new promise with the given numeric value.
    *
    * @param value
    *           numeric promise value
    */
   Promise(final int value) {
      this.value = value;
   }

   /**
    * Returns the numeric value of this promise.
    *
    * @return numeric promise value
    */
   public int getValue() {
      return this.value;
   }
}

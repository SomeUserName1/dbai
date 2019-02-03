/*
 * @(#)WinnerStatus.java   1.0   May 30, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

/**
 * The winner status describes the state of the winner's circle within a group.
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
 */
public enum WinnerStatus {

   /** There is no possibility to satisfy the search context. */
   UNSATISFIABLE(0),
   /** There is a winner that satisfies the search context. */
   SATISFIED(1),
   /** There has been no search for this property and a new search is needed. */
   NEWSEARCH(2),
   /** There has been search for this property but more search is needed. */
   MORESEARCH(3);

   /** Bit mask to emulate the return fields used in Cascades. */
   private int mask;

   /**
    * Creates a new winner status with the given bit mask.
    *
    * @param mask
    *           bit mask
    */
   WinnerStatus(final int mask) {
      this.mask = mask;
   }

   /**
    * Returns whether this status indicates that a new or more search is required.
    *
    * @return {@code true} if search is required, {@code false} otherwise
    */
   public boolean requiresSearch() {
      return (this.mask & 2) > 0;
   }

   /**
    * Returns whether this status indicates that a winner has been found.
    *
    * @return {@code true} if a winner has been found already, {@code false} otherwise
    */
   public boolean foundWinner() {
      return (this.mask & 1) > 0;
   }
}

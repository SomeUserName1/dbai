/*
 * @(#)StatisticalModel.java   1.0   Jun 11, 2014
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
 * Enumeration to model the values of the statistical model that are used for cardinality estimates.
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
public enum StatisticalModel {

   /** Lowest selectivity that will be used in estimates. */
   MINIMUM_SELECTIVITY(0.05),
   /** Selectivity used in equality predicates. */
   EQUALITY_SELECTIVITY(0.1),
   /** Selectivity used in inequality predicates. */
   INEQUALITY_SELECTIVITY(0.5);

   /** Selectivity value. */
   private final double selectivity;

   /**
    * Creates a new statistical model value with the given selectivity.
    *
    * @param selectivity
    *           selectivity value
    */
   StatisticalModel(final double selectivity) {
      this.selectivity = selectivity;
   }

   /**
    * Returns the selectivity of this cost model value.
    *
    * @return selectivity value
    */
   public double getSelectivity() {
      return this.selectivity;
   }
}

/*
 * @(#)LogicalProperties.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import minibase.catalog.Statistics;

/**
 * General logical properties to describe the results of logical query operators.
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
public abstract class LogicalProperties {

   /** Statistics of the described object. */
   private final Statistics statistics;

   /**
    * Creates new logical properties with the given statistics.
    *
    * @param statistics
    *           statistics
    */
   protected LogicalProperties(final Statistics statistics) {
      this.statistics = statistics;
   }

   /**
    * Returns the statistics of the described object.
    *
    * @return statistics
    */
   protected Statistics getStatistics() {
      return this.statistics;
   }

   /**
    * Returns the cardinality of these logical properties.
    *
    * @return cardinality
    */
   public double getCardinality() {
      return this.statistics.getCardinality();
   }

   /**
    * Returns the unique cardinality of these logical properties.
    *
    * @return unique cardinality
    */
   public double getUniqueCardinality() {
      return this.statistics.getUniqueCardinality();
   }
}

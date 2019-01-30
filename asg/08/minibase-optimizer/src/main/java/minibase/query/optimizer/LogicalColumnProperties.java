/*
 * @(#)LogicalColumnProperties.java   1.0   Jul 7, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.Collections;

import minibase.catalog.ColumnStatistics;
import minibase.query.schema.ColumnReference;

/**
 * Logical properties to describe the (column collection element) result of a get-column operator.
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
 * @author Marcel Hanser &lt;marcel.hanser@uni-konstanz.de&gt;
 * @version 1.0
 */
public class LogicalColumnProperties extends LogicalElementProperties {

   /**
    * @param column
    *           column reference
    * @param statistics
    *           column statistics
    */
   public LogicalColumnProperties(final ColumnStatistics statistics, final ColumnReference column) {
      super(new ColumnStatistics(statistics.getUniqueCardinality(), statistics.getUniqueCardinality(),
            statistics.getMinimum(), statistics.getMaximum(), statistics.getWidth()), column.getType(),
            column.getSize(), 0.0, statistics.getUniqueCardinality() == 1, Collections.singletonList(column));
   }

   /**
    * Returns the minimum value of the described column collection element.
    *
    * @return minimum value
    */
   public Object getMinimum() {
      return this.getStatistics().getMinimum();
   }

   /**
    * Returns the maximum value of the described column collection element.
    *
    * @return maximum value
    */
   public Object getMaximum() {
      return this.getStatistics().getMaximum();
   }
}

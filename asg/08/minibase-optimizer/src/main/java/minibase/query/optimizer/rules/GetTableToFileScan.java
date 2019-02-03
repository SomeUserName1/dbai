/*
 * @(#)GetTableToFileScan.java   1.0   Jun 2, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.optimizer.operators.physical.FileScan;

/**
 * Implementation rule that translates the {@code GetTable} logical operator into the {@code FileScan}
 * physical operator, i.e., GetTable(R) -> FileScan(R).
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
public class GetTableToFileScan extends AbstractRule {

   /**
    * Creates a new transformation rule that implements the {@code GetTable} logical operator using the
    * {@code FileScan} physical operator.
    */
   public GetTableToFileScan() {
      super(RuleType.GETTABLE_TO_FILESCAN,
            // Original pattern
            new Expression(new GetTable()),
            // Substitute pattern
            new Expression(new FileScan()));
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final GetTable get = (GetTable) before.getOperator();
      return new Expression(new FileScan(get.getTable(), get.getTable().getColumns()));
   }
}

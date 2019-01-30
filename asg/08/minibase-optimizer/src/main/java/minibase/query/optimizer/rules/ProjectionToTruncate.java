/*
 * @(#)ProjectionToTruncate.java   1.0   Jun 13, 2014
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
import minibase.query.optimizer.operators.logical.Projection;
import minibase.query.optimizer.operators.physical.Truncate;

/**
 * Implementation rule that translates the {@code Projection} logical operator into the {@code Truncate}
 * physical operator, i.e., Projection[l](R) -> Truncate[l](R).
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
public class ProjectionToTruncate extends AbstractRule {

   /**
    * Creates a new transformation rule that implements the {@code Projection} logical operators using the
    * {@code Truncate} physical operator.
    */
   public ProjectionToTruncate() {
      super(RuleType.PROJECTION_TO_TRUNCATE,
            // original pattern
            new Expression(new Projection(), new Expression(new Leaf(0))),
            // substitute pattern
            new Expression(new Truncate(), new Expression(new Leaf(0))));
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final Projection projection = (Projection) before.getOperator();
      return new Expression(new Truncate(projection.getProjectionColumns()), before.getInput(0));
   }
}

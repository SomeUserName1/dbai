/*
 * @(#)EquiJoinToBlockNestedLoopsJoin.java   1.0   Jun 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import java.util.List;

import minibase.query.optimizer.Cost;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.physical.BlockNestedLoopsJoin;
import minibase.query.optimizer.operators.physical.NestedLoopsJoin;
import minibase.query.schema.ColumnReference;

/**
 * Implementation rule that translates the {@code EquiJoin} logical operator into the {@code NestedLoopsJoin}
 * physical operator, i.e., EquiJoin(R, S) -> NestedLoopsJoin(R, S).
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
public class EquiJoinToBlockNestedLoopsJoin extends AbstractRule {

   /**
    * Creates a new transformation rule that implements the {@code EquiJoin} logical operator using the
    * {@code NestedLoopsJoin} physical operator.
    */
   public EquiJoinToBlockNestedLoopsJoin() {
      super(RuleType.EQUI_TO_BLOCKNESTEDLOOPS_JOIN,
            // Original pattern
            new Expression(new EquiJoin(), new Expression(new Leaf(0)), new Expression(new Leaf(1))),
            // Substitute pattern
            new Expression(new BlockNestedLoopsJoin(), new Expression(new Leaf(0)),
                  new Expression(new Leaf(1))));
   }

   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      // TODO This code could be factored out into a common super class.
      final Cost leftInputCost = expression.getInput(0).getLowerBound();
      final Cost rightInputCost = expression.getInput(1).getLowerBound();
      if (leftInputCost.plus(rightInputCost).compareTo(context.getUpperBound()) >= 0) {
         // Do not explore this plan because one of its input groups has been pruned.
         return false;
      }
      return true;
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final EquiJoin equiJoin = (EquiJoin) before.getOperator();
      final List<ColumnReference> leftColumnReferences = equiJoin.getLeftColumns();
      final List<ColumnReference> rightColumnReferences = equiJoin.getRightColumns();
      return new Expression(new NestedLoopsJoin(leftColumnReferences, rightColumnReferences),
            before.getInput(0), before.getInput(1));
   }
}

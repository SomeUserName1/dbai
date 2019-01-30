/*
 * @(#)EquiJoinLeftToRight.java   1.0   May 9, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;

/**
 * Transformation rule that represents the fact that the join operation is left-to-right associative. If
 * applied, this rule will transform an expression as follows: EquiJoin(EquiJoin(A, B), C) -> EquiJoin(A,
 * EquiJoin(B, C)).
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
public class EquiJoinLeftToRight extends AbstractRule {

   /**
    * Constructs a new transformation rule that applies left-to-right join associativity.
    */
   public EquiJoinLeftToRight() {
      super(RuleType.EQUI_JOIN_LTOR,
            // bit-mask of this rule
            1 << RuleType.EQUI_JOIN_LTOR.ordinal() | 1 << RuleType.EQUI_JOIN_RTOL.ordinal()
                  | 1 << RuleType.EQUI_JOIN_EXCHANGE.ordinal(),
            // original pattern
            new Expression(new EquiJoin(),
                  new Expression(new EquiJoin(), new Expression(new Leaf(0)), new Expression(new Leaf(1))),
                  new Expression(new Leaf(2))),
            // substitute pattern
            new Expression(new EquiJoin(), new Expression(new Leaf(0)),
                  new Expression(new EquiJoin(), new Expression(new Leaf(1)), new Expression(new Leaf(2)))));
   }

   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      if (expression.getGroup().getOptimizer().getSettings().isAllowingCrossJoin()) {
         return true;
      }
      // Get the columns of the left-hand side of the parent join condition
      final EquiJoin parent = (EquiJoin) before.getOperator();
      final List<ColumnReference> leftColumnRefs = parent.getLeftColumns();
      // Get the schema of the "middle" input
      final Group group = ((Leaf) before.getInput(0).getInput(1).getOperator()).getGroup();
      final Schema schema = ((LogicalCollectionProperties) group.getLogicalProperties()).getSchema();
      // Check if there is a join condition in the parent join that uses a column from the "middle" input.
      for (final ColumnReference columnRef : leftColumnRefs) {
         if (schema.containsColumn(columnRef)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      // join2: second (top) join
      final EquiJoin join2 = (EquiJoin) before.getOperator();
      final List<ColumnReference> leftColumnRefs2 = join2.getLeftColumns();
      final List<ColumnReference> rightColumnRefs2 = join2.getRightColumns();

      final Expression joinAB = before.getInput(0);
      // join1: first (nested) join
      final EquiJoin join1 = (EquiJoin) joinAB.getOperator();
      final List<ColumnReference> leftColumnRefs1 = join1.getLeftColumns();
      final List<ColumnReference> rightColumnRefs1 = join1.getRightColumns();

      // calculate new join conditions for second join
      final List<ColumnReference> newLeftColumnRefs2 = new ArrayList<>(leftColumnRefs1);
      final List<ColumnReference> newRightColumnRefs2 = new ArrayList<>(rightColumnRefs1);

      // calculate new join conditions for first join
      final List<ColumnReference> newLeftColumnRefs1 = new ArrayList<>();
      final List<ColumnReference> newRightColumnRefs1 = new ArrayList<>();

      final Leaf opB = (Leaf) joinAB.getInput(1).getOperator();
      final Group groupB = opB.getGroup();
      final Schema schemaB = ((LogicalCollectionProperties) groupB.getLogicalProperties()).getSchema();
      for (int i = 0; i < leftColumnRefs2.size(); i++) {
         final ColumnReference leftColumnRef = leftColumnRefs2.get(i);
         final ColumnReference rightColumnRef = rightColumnRefs2.get(i);
         if (schemaB.containsColumn(leftColumnRef)) {
            // join column is in B: assign join condition to first join
            newLeftColumnRefs1.add(leftColumnRef);
            newRightColumnRefs1.add(rightColumnRef);
         } else {
            // join column is in A: assign join condition to second join
            newLeftColumnRefs2.add(leftColumnRef);
            newRightColumnRefs2.add(rightColumnRef);
         }
      }
      return new Expression(new EquiJoin(newLeftColumnRefs2, newRightColumnRefs2), joinAB.getInput(0),
            new Expression(new EquiJoin(newLeftColumnRefs1, newRightColumnRefs1), joinAB.getInput(1),
                  before.getInput(1)));
   }
}

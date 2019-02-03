/*
 * @(#)EquiJoinToIndexNestedLoopsJoin.java   1.0   Jun 20, 2014
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

import minibase.catalog.IndexDescriptor;
import minibase.catalog.TableDescriptor;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.optimizer.operators.physical.IndexNestedLoopsJoin;
import minibase.query.schema.ColumnReference;

/**
 * Implementation rule that translates the {@code EquiJoin} logical operator into the
 * {@code IndexNestedLoopsJoin} physical operator, i.e., EquiJoin(R, S) -> IndexNestedLoopsJoin(R, I(S)).
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
public class EquiJoinToIndexNestedLoopsJoin extends AbstractRule {

   /**
    * Creates a new transformation rule that implements the {@code EquiJoin} logical operator using the
    * {@code IndexNestedLoopsJoin} physical operator.
    */
   public EquiJoinToIndexNestedLoopsJoin() {
      super(RuleType.EQUI_TO_IDXNESTEDLOOPS_JOIN,
            // Original pattern
            new Expression(new EquiJoin(), new Expression(new Leaf(0)), new Expression(new GetTable())),
            // Substitute pattern
            new Expression(new IndexNestedLoopsJoin(), new Expression(new Leaf(0))));
   }

   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      final EquiJoin equiJoin = (EquiJoin) before.getOperator();
      final GetTable getTable = (GetTable) before.getInput(1).getOperator();
      final TableDescriptor descriptor = getTable.getTable().getDescriptor();
      // Check all indexes defined for the right table.
      for (final IndexDescriptor index : descriptor.getIndexes()) {
         // Check whether the right column of the join predicate match the index key.
         if (index.getKey().isMatch(equiJoin.getRightColumns())) {
            return true;
         }
      }
      return false;
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final EquiJoin equiJoin = (EquiJoin) before.getOperator();
      final GetTable getTable = (GetTable) before.getInput(1).getOperator();
      final List<ColumnReference> leftColumnReferences = equiJoin.getLeftColumns();
      final List<ColumnReference> rightColumnReferences = equiJoin.getRightColumns();
      return new Expression(
            new IndexNestedLoopsJoin(leftColumnReferences, rightColumnReferences, getTable.getTable()),
            before.getInput(0));
   }
}

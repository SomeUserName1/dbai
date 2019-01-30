/*
 * @(#)AggregationThroughEquiJoin.java   1.0   Jun 13, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import java.util.Collection;
import java.util.List;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.Aggregation;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.DerivedColumnReference;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaKey;

/**
 * Transformation rule that pushes a {@code Aggregation} logical operator through a {@code EquiJoin} logical
 * operator, i.e., Aggregation[G, A](EquiJoin(R, S)) -> EquiJoin(R, Aggregation[G, A](S)).
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
public class AggregationThroughEquiJoin extends AbstractRule {

   /**
    * Creates a new transformation rule that pushes a {@code Aggregation} logical operator through a
    * {@code EquiJoin} logical operator.
    */
   public AggregationThroughEquiJoin() {
      super(RuleType.AGGREGATION_THROUGH_EQUIJOIN,
            // original pattern
            new Expression(new Aggregation(),
                  new Expression(new EquiJoin(), new Expression(new Leaf(0)), new Expression(new Leaf(1)))),
            // substitute pattern
            new Expression(new EquiJoin(), new Expression(new Leaf(0)),
                  new Expression(new Aggregation(), new Expression(new Leaf(1)))));
   }

   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      // Conditions for this rule to be applicable
      // 1. All columns used in the aggregates must be in the right schema.
      // 2. The columns reference in the equi-join predicate are in the group-by list.
      // 3. The equi-join is a foreign key join (verified by checking if left columns contain a candidate key
      // of the left input.

      // Get Aggregation operator, group-by columns, and aggregated columns.
      final Aggregation aggregation = (Aggregation) before.getOperator();
      final List<ColumnReference> groupByColumns = aggregation.getGroupByColumns();
      final List<DerivedColumnReference> aggregatedColumns = aggregation.getAggregatedColumns();

      // Get EquiJoin operator, left and right join columns.
      final EquiJoin join = (EquiJoin) before.getInput(0).getOperator();
      final List<ColumnReference> leftColumns = join.getLeftColumns();
      final List<ColumnReference> rightColumns = join.getRightColumns();

      // Get the schema of the right input
      final Group rightGroup = ((Leaf) before.getInput(0).getInput(1).getOperator()).getGroup();
      final LogicalCollectionProperties rightProperties = (LogicalCollectionProperties) rightGroup
            .getLogicalProperties();
      final Schema rightSchema = rightProperties.getSchema();

      // Get candidate keys of the left input
      final Group leftGroup = ((Leaf) before.getInput(0).getInput(0).getOperator()).getGroup();
      final LogicalCollectionProperties leftProperties = (LogicalCollectionProperties) leftGroup
            .getLogicalProperties();
      final Collection<SchemaKey> candidateKeys = leftProperties.getSchema().getCandidateKeys();

      // Condition 1: check that all columns used in aggregates are contained in the right schema
      for (final DerivedColumnReference aggregatedColumn : aggregatedColumns) {
         for (final ColumnReference inputColumn : aggregatedColumn.getInputColumns()) {
            if (!rightSchema.containsColumn(inputColumn)) {
               return false;
            }
         }
      }
      for (final ColumnReference groupByColumn : groupByColumns) {
         if (!rightSchema.containsColumn(groupByColumn)) {
            return false;
         }
      }

      // Condition 2: check that the right join columns are all contained in the group-by columns
      for (final ColumnReference rightColumn : rightColumns) {
         if (!groupByColumns.contains(rightColumn)) {
            return false;
         }
      }
      // Condition 3: check that the left join columns contains a candidate key of the left input
      for (final SchemaKey candidateKey : candidateKeys) {
         if (candidateKey.isSubsetOf(leftColumns)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final Aggregation aggregation = (Aggregation) before.getOperator();
      final EquiJoin join = (EquiJoin) before.getInput(0).getOperator();
      return new Expression(join, before.getInput(0).getInput(0),
            new Expression(aggregation, before.getInput(0).getInput(1)));
   }
}

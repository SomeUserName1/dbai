/*
 * @(#)ProjectionThroughEquiJoin.java   1.0   Aug 16, 2014
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.logical.Projection;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;

/**
 * Transformation rule that pushes a {@code Projection} logical operator through a {@code EquiJoin} logical
 * operator, i.e., Projection[l](EquiJoin(R, S)) -> Projection[l1](EquiJoin(Projection[l2](R),
 * Projection[l3](S))).
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
public class ProjectionThroughEquiJoin extends AbstractRule {

   /**
    * Creates a new transformation rule that pushes a {@code Projection} through an {@code EquiJoin} logical
    * operator.
    */
   public ProjectionThroughEquiJoin() {
      super(RuleType.PROJECTION_THROUGH_EQUIJOIN,
            // original expression
            new Expression(new Projection(),
                  new Expression(new EquiJoin(), new Expression(new Leaf(0)), new Expression(new Leaf(1)))),
            // substitute expression
            new Expression(new Projection(),
                  new Expression(new EquiJoin(),
                        new Expression(new Projection(), new Expression(new Leaf(0))),
                        new Expression(new Projection(), new Expression(new Leaf(1))))));
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      // Get the projection list of the original projection operator.
      final Projection projection = (Projection) before.getOperator();
      final List<ColumnReference> projectionColumns = projection.getProjectionColumns();
      // Get left and right input of original equijoin operator.
      final Expression leftInput = before.getInput(0).getInput(0);
      final Expression rightInput = before.getInput(0).getInput(1);
      // Get schema of left input of original join.
      final Group leftGroup = ((Leaf) leftInput.getOperator()).getGroup();
      final Schema leftSchema = ((LogicalCollectionProperties) leftGroup.getLogicalProperties()).getSchema();
      // Create new projection lists.
      final Set<ColumnReference> leftProjectionColumns = new HashSet<>();
      final Set<ColumnReference> rightProjectionColumns = new HashSet<>();
      // Get the join predicate of the original equijoin operator.
      final EquiJoin originalEquiJoin = (EquiJoin) before.getInput(0).getOperator();
      leftProjectionColumns.addAll(originalEquiJoin.getLeftColumns());
      rightProjectionColumns.addAll(originalEquiJoin.getRightColumns());
      for (final ColumnReference projectionColumn : projectionColumns) {
         if (leftSchema.containsColumn(projectionColumn)) {
            leftProjectionColumns.add(projectionColumn);
         } else {
            rightProjectionColumns.add(projectionColumn);
         }
      }
      return new Expression(projection,
            new Expression(originalEquiJoin,
                  new Expression(new Projection(new ArrayList<>(leftProjectionColumns)), leftInput),
                  new Expression(new Projection(new ArrayList<>(rightProjectionColumns)), rightInput)));
   }
}

/*
 * @(#)ProjectionThroughSelection.java   1.0   Jun 22, 2014
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
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.logical.Projection;
import minibase.query.optimizer.operators.logical.Selection;
import minibase.query.schema.ColumnReference;

/**
 * Transformation rule that pushes a {@code Projection} logical operator through a {@code Selection} logical
 * operator, i.e., Projection[l](Selection(R)) -> Projection[l1](Selection(Projection[l2](R))).
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
public class ProjectionThroughSelection extends AbstractRule {

   /**
    * Creates a new transformation rule that pushes a projection through a selection logical operator.
    */
   public ProjectionThroughSelection() {
      super(RuleType.PROJECTION_THROUGH_SELECTION,
            // original expression
            new Expression(new Projection(),
                  new Expression(new Selection(), new Expression(new Leaf(0)), new Expression(new Leaf(1)))),
            // substitute expression
            new Expression(new Projection(),
                  new Expression(new Selection(),
                        new Expression(new Projection(), new Expression(new Leaf(0))),
                        new Expression(new Leaf(1)))));
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      // Get the projection list of the original projection operator.
      final Projection originalProjection = (Projection) before.getOperator();
      final List<ColumnReference> originalColumns = originalProjection.getProjectionColumns();
      // Get the selection predicate and the free variables.
      final Leaf predicate = (Leaf) before.getInput(0).getInput(1).getOperator();
      final Group predicateGroup = predicate.getGroup();
      final LogicalElementProperties logicalProperties = (LogicalElementProperties) predicateGroup
            .getLogicalProperties();
      final List<ColumnReference> freeVariables = logicalProperties.getInputColumns();
      // Create the projection list for the new projection operator.
      final List<ColumnReference> newColumns = new ArrayList<>(originalColumns);
      for (final ColumnReference freeVariable : freeVariables) {
         if (!newColumns.contains(freeVariable)) {
            newColumns.add(freeVariable);
         }
      }
      // Create and return transformed expression
      return new Expression(new Projection(originalColumns),
            new Expression(new Selection(),
                  new Expression(new Projection(newColumns), before.getInput(0).getInput(0)),
                  before.getInput(0).getInput(1)));
   }
}

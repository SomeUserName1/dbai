/*
 * @(#)SelectionToIndexFilter.java   1.0   Jun 22, 2014
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
import minibase.query.optimizer.Group;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.optimizer.operators.logical.Selection;
import minibase.query.optimizer.operators.physical.IndexFilter;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.StoredTableReference;

/**
 * Implementation rule that translates the {@code Selection} logical operator into the {@code IndexFilter}
 * physical operator, i.e., Selection(R, p) -> IndexFilter(R, p).
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
public class SelectionToIndexFilter extends AbstractRule {

   /**
    * Creates a new transformation rule that implements the {@code Selection} logical operators using the
    * {@code IndexFilter} physical operator.
    */
   public SelectionToIndexFilter() {
      super(RuleType.SELECTION_TO_IDXFILTER,
            // original pattern
            new Expression(new Selection(), new Expression(new GetTable()), new Expression(new Leaf(0))),
            // substitute pattern
            new Expression(new IndexFilter(), new Expression(new Leaf(0))));
   }

   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      // Get the selection predicate and the free variables.
      final Leaf predicate = (Leaf) before.getInput(1).getOperator();
      final Group predicateGroup = predicate.getGroup();
      final LogicalElementProperties logicalProperties = (LogicalElementProperties) predicateGroup
            .getLogicalProperties();
      final List<ColumnReference> freeVariables = logicalProperties.getInputColumns();
      // Assert that there is only one free variable.
      if (freeVariables.size() != 1) {
         return false;
         // TODO Index filter operator currently only supports single variable predicates.
      }
      // Get the list of indexes that are defined for the table accessed in the selection.
      final StoredTableReference tableRef = ((GetTable) before.getInput(0).getOperator()).getTable();
      final TableDescriptor tableDescriptor = tableRef.getDescriptor();
      for (final IndexDescriptor indexDescriptor : tableDescriptor.getIndexes()) {
         if (indexDescriptor.getKey().isMatch(freeVariables)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      return new Expression(new IndexFilter(((GetTable) before.getInput(0).getOperator()).getTable()),
            before.getInput(1));
   }
}

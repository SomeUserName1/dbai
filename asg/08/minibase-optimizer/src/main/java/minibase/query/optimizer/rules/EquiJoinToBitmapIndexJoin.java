/*
 * @(#)EquiJoinToBitmapIndexJoin.java   1.0   Jun 27, 2014
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

import minibase.catalog.BitmapIndexDescriptor;
import minibase.catalog.IndexType;
import minibase.catalog.TableDescriptor;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.optimizer.operators.logical.Selection;
import minibase.query.optimizer.operators.physical.BitmapIndexJoin;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.SchemaKey;

/**
 * Implementation rule that translates the {@code EquiJoin} logical operator into the {@code BitIndexJoin}
 * physical operator, i.e., EquiJoin(R, S) -> BitIndexJoin(R, I(S)). In order to apply this rule, the
 * following conditions need to be satisfied.
 * <ol>
 * <li>The equi-join condition is a <strong>single</strong> equality predicate.</li>
 * <li>The index attribute is a <strong>candidate key</strong> of the left input.</li>
 * <li>The equi-join is a <strong>foreign key join</strong>, with the left attribute as the foreign key and
 * the right attribute as the candidate of a table {@code T}.</li>
 * <li>The free variable of the predicate is <strong>equal</strong> to the attribute of {@code T} that is
 * indexed by the bit index <em>and</em> the predicate of the selection <strong>matches</strong> the predicate
 * of the bit index.</li>
 * </ol>
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
public class EquiJoinToBitmapIndexJoin extends AbstractRule {

   /**
    * Creates a new transformation rule that implements the {@code EquiJoin} logical operator using the
    * {@code BitIndexJoin} physical operator.
    */
   public EquiJoinToBitmapIndexJoin() {
      super(RuleType.EQUI_TO_BITMAPIDX_JOIN,
            // Original pattern
            new Expression(new EquiJoin(), new Expression(new Leaf(0)),
                  new Expression(new Selection(), new Expression(new GetTable()),
                        new Expression(new Leaf(1)))),
            // Substitute pattern
            new Expression(new BitmapIndexJoin(), new Expression(new Leaf(0)), new Expression(new Leaf(1))));
   }

   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      final EquiJoin equiJoin = (EquiJoin) before.getOperator();
      final List<ColumnReference> leftColumnRefs = equiJoin.getLeftColumns();
      final List<ColumnReference> rightColumnRefs = equiJoin.getRightColumns();
      // Get the left input of the equi-join.
      final Leaf leftInput = (Leaf) before.getInput(0).getOperator();
      final LogicalCollectionProperties leftProperties = (LogicalCollectionProperties) leftInput.getGroup()
            .getLogicalProperties();
      // Get candidate keys of left input.
      final Collection<SchemaKey> leftCandidateKeys = leftProperties.getSchema().getCandidateKeys();
      // Get the input to Selection, i.e., the right collection.
      final Expression rightInput = before.getInput(1).getInput(0);
      final TableDescriptor tableDescriptor = ((GetTable) rightInput.getOperator()).getTable()
            .getDescriptor();
      // Get candidates keys of right input.
      final Collection<SchemaKey> rightCandidateKeys = rightInput.getSchema().getCandidateKeys();
      // Get the predicate of the Selection.
      final Leaf predicate = (Leaf) before.getInput(1).getInput(1).getOperator();
      final LogicalElementProperties rightProperties = (LogicalElementProperties) predicate.getGroup()
            .getLogicalProperties();
      // Get the free variables of the selection predicate.
      final List<ColumnReference> freeVariables = rightProperties.getInputColumns();
      // Check Condition #1: does the euqi-joins use a single equality predicate?
      if (leftColumnRefs.size() == 1 && rightColumnRefs.size() == 1 && leftCandidateKeys.size() > 0) {
         for (final BitmapIndexDescriptor index : tableDescriptor.getBitmapIndexes()) {
            // Skip all indexes other than bitmap join indexes.
            if (!IndexType.BITMAP_JOIN.equals(index.getIndexType())) {
               continue;
            }
            // Check Condition #2: are the columns references in the join condition used to built the bitmap
            // join index a candidate key of the left input?
            if (!leftCandidateKeys.containsAll(index.getReferences())) {
               continue;
            }
            // Check Condition #3: is the right candidate key a subset of the right attributes of the join
            // condition?
            boolean found = false;
            for (final SchemaKey rightCandidateKey : rightCandidateKeys) {
               if (rightCandidateKey.isSubsetOf(rightColumnRefs)) {
                  found = true;
               }
            }
            if (!found) {
               return false;
            }
            // Check Condition #4: do the free variables of the predicate match the key of the bitmap join
            // index?
            if (SchemaKey.resolve(index.getKey(), freeVariables).isPresent()) {
               return true;
            }
            // TODO Check if the predicate matches the predicate of the index.
            // predicate.equals(index.getPredicate());
         }
      }
      return false;
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final EquiJoin equiJoin = (EquiJoin) before.getOperator();
      final List<ColumnReference> leftColumnRefs = equiJoin.getLeftColumns();
      final List<ColumnReference> rightColumnRefs = equiJoin.getRightColumns();
      final GetTable getTable = (GetTable) before.getInput(1).getInput(0).getOperator();
      return new Expression(new BitmapIndexJoin(leftColumnRefs, rightColumnRefs, getTable.getTable()),
            before.getInput(0), before.getInput(1).getInput(1));
   }
}

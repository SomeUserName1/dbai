/*
 * @(#)IndexFilter.java   1.0   Jun 22, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import java.util.List;

import minibase.catalog.IndexDescriptor;
import minibase.catalog.IndexType;
import minibase.catalog.TableDescriptor;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.StoredTableReference;

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.Selection
 * Selection} logical operator using an index.
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
public class IndexFilter extends AbstractPhysicalOperator {

   /** Reference to the table. */
   private final StoredTableReference table;

   /**
    * Creates a new index filter physical operator that filters the given table using an index.
    *
    * @param table
    *           table reference
    */
   public IndexFilter(final StoredTableReference table) {
      super(OperatorType.IDXFILTER);
      this.table = table;
   }

   /**
    * Constructs a new {@code IndexFilter} operator. This variant of the constructor is used by the rule
    * engine to create a template of this operator. Therefore, all its fields are initialized to {@code null}.
    */
   public IndexFilter() {
      this(null);
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final TableDescriptor descriptor = this.table.getDescriptor();
      final double inputWidth = descriptor.getStatistics().getWidth();
      final double inputCardinality = descriptor.getStatistics().getCardinality();
      final double outputCardinality = ((LogicalCollectionProperties) localProperties).getCardinality();
      final List<ColumnReference> freeVariables = ((LogicalElementProperties) inputProperties[0])
            .getInputColumns();
      // Assert that there is only one free variable.
      // TODO discuss this later, originally commented in.
      // if (freeVariables.size() != 1) {
      // throw new
      // OptimizerError("Index filter operator currently only supports single variable predicates.");
      // }
      // Determine the best possible index to use.
      boolean clustered = false;
      for (final IndexDescriptor indexDescriptor : descriptor.getIndexes()) {
         if (indexDescriptor.getKey().isMatch(freeVariables)
               && IndexType.BTREE.equals(indexDescriptor.getIndexType()) && indexDescriptor.isClustered()) {
            clustered = true;
            break;
         }
      }
      if (clustered) {
         // Found a clustered B-tree index. Assuming a single range predicate, e.g., 18 <= age <= 64, index
         // filter uses the following algorithm to retrieve matching tuples. It begins by looking up the first
         // tuple that matches the condition, i.e., age = 18. Then it reads all tuples in the range, until a
         // tuple is encountered for which the condition is no longer satisfied, i.e., age > 64.

         // Read a single page of the index.
         double cpuCost = CostModel.CPU_READ.getCost();
         double ioCost = CostModel.IO.getCost();
         // Read blocks until condition is no longer satisfied.
         final double indexBlocks = Math.ceil(outputCardinality * inputWidth);
         cpuCost += indexBlocks * CostModel.CPU_READ.getCost();
         ioCost += indexBlocks * CostModel.IO.getCost();
         // Evaluate the predicate for each tuple that is read.
         cpuCost += outputCardinality * CostModel.CPU_PRED.getCost();
         return new Cost(ioCost, cpuCost);
      }
      // No clustered B-tree index found. Index filter needs to evaluate the predicate on an unclustered index
      // by looking up each tuple in the range that satisfies the predicate individually. If the number of
      // resulting tuples is small, the block in which the tuple is stored is read directly for each tuple. If
      // the number of resulting tuples is large, all record IDs are gathered first and sorted. Then the
      // blocks in which the tuples are read sequentially.

      // Read as many index blocks as necessary.
      double indexBlocks = Math.ceil(outputCardinality / CostModel.INDEX_BF.getCost());
      double cpuCost = indexBlocks * CostModel.CPU_READ.getCost();
      double ioCost = indexBlocks * CostModel.IO.getCost();
      // Read all data blocks or read data for every index entry found.
      indexBlocks = Math.min(Math.ceil(inputCardinality * inputWidth), outputCardinality);
      cpuCost += indexBlocks * CostModel.CPU_READ.getCost();
      ioCost += indexBlocks * CostModel.IO.getCost();
      return new Cost(ioCost, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      this.assertInputNo(inputNo);
      inputRequiredProperties.set(PhysicalProperties.anyPhysicalProperties());
      return true;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      result.append(this.table.getName());
      result.append("]");
      return result.toString();
   }
}

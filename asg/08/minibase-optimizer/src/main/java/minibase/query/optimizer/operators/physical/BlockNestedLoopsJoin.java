/*
 * @(#)BlockNestedLoopsJoin.java   1.0   Jun 3, 2014
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

import minibase.catalog.DataOrder;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.EquiJoin EquiJoin}
 * logical operator using the simple nested-loops join algorithm.
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
public class BlockNestedLoopsJoin extends AbstractPhysicalJoin {

   /**
    * Creates a new {@code NestedLoopsJoin} physical operator. The join predicate is given in the form of two
    * sets of the same size. Each set contains a number of references to columns in the system catalog.
    * Columns are assumed to be pair-wise equal, i.e., the join predicate is
    * {@code leftColumns[1] = rightColumns[1] AND} ... {@code AND leftColumns[n] = rightColumns[n]}. If both
    * sets of column reference IDs are empty, a natural join or a cross-product is performed.
    *
    * @param leftColumns
    *           left columns
    * @param rightColumns
    *           right columns
    */
   public BlockNestedLoopsJoin(final List<ColumnReference> leftColumns,
         final List<ColumnReference> rightColumns) {
      super(OperatorType.NLJOIN, leftColumns, rightColumns);
   }

   /**
    * Constructs a new {@code NestedLoopsJoin} physical operator. This variant of the constructor is used by
    * the rule engine to create a template of a this operator. Therefore, all its fields are initialized to
    * {@code null}.
    */
   public BlockNestedLoopsJoin() {
      super(OperatorType.NLJOIN);
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final double outputCardinality = ((LogicalCollectionProperties) localProperties).getCardinality();
      final double leftCardinality = ((LogicalCollectionProperties) inputProperties[0]).getCardinality();
      final LogicalCollectionProperties rightProperties = (LogicalCollectionProperties) inputProperties[1];
      final double rightCardinality = rightProperties.getCardinality();
      // Compute CPU cost
      double cpuCost = leftCardinality * rightCardinality * CostModel.CPU_PRED.getCost();
      cpuCost += outputCardinality * CostModel.TOUCH_COPY.getCost();
      // Compute I/O cost
      double ioCost = 0;
      final double rightBlocks = Math.ceil(rightCardinality * rightProperties.getWidth());
      // Check whether the right input needs to be materialized.
      if (!rightProperties.isTable()) {
         ioCost += rightBlocks;
      }
      ioCost += leftCardinality * rightBlocks * (CostModel.IO.getCost() + CostModel.CPU_READ.getCost());
      return new Cost(ioCost, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      this.assertInputNo(inputNo);
      if (!DataOrder.ANY.equals(requiredProperties.getOrder())) {
         if (inputNo == 0) {
            // The left schema should include the property keys
            final Schema schema = ((LogicalCollectionProperties) inputLogicalProperties).getSchema();
            if (!schema.containsKey(requiredProperties.getKey())) {
               inputRequiredProperties.set(null);
               return false;
            }
         }
      }
      if (inputNo == 0) {
         // Use properties of the left input
         inputRequiredProperties.set(requiredProperties);
      } else {
         // No required properties for the right input
         inputRequiredProperties.set(PhysicalProperties.anyPhysicalProperties());
      }
      return true;
   }
}

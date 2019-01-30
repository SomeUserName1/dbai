/*
 * @(#)Filter.java   1.0   Jun 12, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import minibase.catalog.DataOrder;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.Selection
 * Selection} logical operator.
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
public class Filter extends AbstractPhysicalOperator {

   /**
    * Creates a new {@code Filter} physical operator.
    */
   public Filter() {
      super(OperatorType.FILTER);
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final double inputCardinality = ((LogicalCollectionProperties) inputProperties[0]).getCardinality();
      final double outputCardinality = ((LogicalCollectionProperties) localProperties).getCardinality();
      // Compute CPU cost.
      final double cpuCost = inputCardinality * CostModel.CPU_PRED.getCost()
            + outputCardinality * CostModel.TOUCH_COPY.getCost();
      // Filter has no I/O cost.
      return new Cost(0.0, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      this.assertInputNo(inputNo);
      // Right input is an element group and therefore has no required properties.
      if (inputNo == 1) {
         inputRequiredProperties.set(PhysicalProperties.anyPhysicalProperties());
         return true;
      }
      // Left input is a normal group and therefore required properties can be computed.
      if (!DataOrder.ANY.equals(requiredProperties.getOrder())) {
         if (!((LogicalCollectionProperties) inputLogicalProperties).getSchema()
               .containsKey(requiredProperties.getKey())) {
            return false;
         }
      }
      inputRequiredProperties.set(requiredProperties);
      return true;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName();
   }
}

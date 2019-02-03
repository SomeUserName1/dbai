/*
 * @(#)Truncate.java   1.0   Jun 13, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import java.util.Arrays;
import java.util.Collections;
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

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.Projection
 * Projection} logical operator.
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
public class Truncate extends AbstractPhysicalOperator {

   /** List of projection columns. */
   private final List<ColumnReference> projectionList;

   /**
    * Creates a new {@code Truncate} physical operator that truncates tuples to the columns in the given
    * projection list.
    *
    * @param projectionList
    *           list of projection columns
    */
   @SafeVarargs
   public Truncate(final ColumnReference... projectionList) {
      this(Arrays.asList(projectionList));
   }

   /**
    * Creates a new {@code Projection} logical operator that projects tuples to the columns in the given
    * projection list.
    *
    * @param projectionList
    *           list of projection columns
    */
   public Truncate(final List<ColumnReference> projectionList) {
      super(OperatorType.TRUNCATE);
      if (projectionList != null) {
         this.projectionList = projectionList;
      } else {
         this.projectionList = Collections.emptyList();
      }
   }

   /**
    * Returns the list of projection columns used by this projection operator.
    *
    * @return list of projection columns
    */
   public List<ColumnReference> getProjectionList() {
      return this.projectionList;
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final double inputCardinality = ((LogicalCollectionProperties) inputProperties[0]).getCardinality();
      final double outputCardinality = ((LogicalCollectionProperties) localProperties).getCardinality();
      @SuppressWarnings("unused")
      final double ratio = Math.min(inputCardinality, outputCardinality)
            / Math.max(inputCardinality, outputCardinality);

      // TODO: The following assertion is violated due to inconsistent join cardinality estimates.
      // assert ratio < 0.999999 : "Input cardinality does not match output cardinality.";

      // Compute CPU cost.
      final double cpuCost = inputCardinality * CostModel.TOUCH_COPY.getCost();
      // Truncate has no I/O cost.
      return new Cost(0.0, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      this.assertInputNo(inputNo);
      if (!DataOrder.ANY.equals(requiredProperties.getOrder())) {
         // If order is required, the keys need to be in the input logical properties.
         if (!((LogicalCollectionProperties) inputLogicalProperties).getSchema()
               .containsKey(requiredProperties.getKey())) {
            return false;
         }
      }
      // Pass the required properties to the inputs
      inputRequiredProperties.set(requiredProperties);
      return true;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      for (int i = 0; i < this.projectionList.size(); i++) {
         result.append(this.projectionList.get(i).getName());
         if (i + 1 < this.projectionList.size()) {
            result.append(", ");
         }
      }
      result.append("]");
      return result.toString();
   }
}

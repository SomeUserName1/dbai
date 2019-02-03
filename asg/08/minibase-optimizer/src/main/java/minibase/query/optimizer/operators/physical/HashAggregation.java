/*
 * @(#)HashAggregation.java   1.0   Jun 11, 2016
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
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.DerivedColumnReference;
import minibase.query.schema.SchemaKey;

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.Aggregation
 * Aggregation} logical operator by hashing all groups.
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
public class HashAggregation extends AbstractPhysicalOperator {

   /** Group-by columns of this aggregation operator. */
   private final List<ColumnReference> groupByColumns;

   /** Columns aggregated by this aggregation operator. */
   private final List<DerivedColumnReference> aggregatedColumns;

   /**
    * Creates a new {@code HashAggregation} physical operator that uses hashing to group rows and compute
    * aggregated values.
    *
    * @param groupByColumns
    *           group-by columns
    * @param aggregatedColumns
    *           aggregated columns
    */
   public HashAggregation(final List<ColumnReference> groupByColumns,
         final List<DerivedColumnReference> aggregatedColumns) {
      super(OperatorType.HASHAGGREGATE);
      this.groupByColumns = groupByColumns;
      this.aggregatedColumns = aggregatedColumns;
   }

   /**
    * Constructs a new {@code HashAggregation} operator. This variant of the constructor is used by the rule
    * engine to create a template of this operator. Therefore, all its fields are initialized to {@code null}.
    */
   public HashAggregation() {
      super(OperatorType.HASHAGGREGATE);
      this.groupByColumns = null;
      this.aggregatedColumns = null;
   }

   /**
    * Returns the group-by columns used by this aggregation operator.
    *
    * @return group-by columns
    */
   public List<ColumnReference> getGroupByColumns() {
      return this.groupByColumns;
   }

   /**
    * Returns the aggregated columns produced by this aggregation operator.
    *
    * @return aggregated columns
    */
   public List<DerivedColumnReference> getAggregatedColumns() {
      return this.aggregatedColumns;
   }

   @Override
   public PhysicalProperties getPhysicalProperties(final PhysicalProperties... inputProperties) {
      return new PhysicalProperties(DataOrder.HASHED, new SchemaKey(this.groupByColumns), null);
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final double inputCardinality = inputProperties[0].getCardinality();
      final double outputCardinality = localProperties.getCardinality();

      // TODO Really? No I/O Costs?
      final double ioCost = 0.0;
      double cpuCost = inputCardinality
            * (CostModel.HASH_COST.getCost() + CostModel.CPU_APPLY.getCost() * this.aggregatedColumns.size());
      cpuCost += outputCardinality * CostModel.TOUCH_COPY.getCost();
      return new Cost(ioCost, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      this.assertInputNo(inputNo);
      // Pass the required properties to the inputs
      inputRequiredProperties.set(requiredProperties);
      return true;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      for (int i = 0; i < this.aggregatedColumns.size(); i++) {
         result.append(this.aggregatedColumns.get(i).getName());
         if (i + 1 < this.aggregatedColumns.size()) {
            result.append(", ");
         }
      }
      if (this.groupByColumns != null && this.groupByColumns.size() > 0) {
         result.append(", GroupBy[");
         for (int i = 0; i < this.groupByColumns.size(); i++) {
            result.append(this.groupByColumns.get(i).getName());
            if (i + 1 < this.groupByColumns.size()) {
               result.append(", ");
            }
         }
         result.append("]");
      }
      result.append("]");
      return result.toString();
   }
}

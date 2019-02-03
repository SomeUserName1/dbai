/*
 * @(#)Sort.java   1.0   Jun 14, 2014
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
import minibase.catalog.SortOrder;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;

/**
 * Physical enforcer operator that sorts its input.
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
 * @author Devarsh Karekal Umashankar &lt;devarsh.karekal-umashankar@uni-konstanz.de&gt;
 * @version 1.0
 */
public class Sort extends AbstractPhysicalOperator {

   /** References to sort columns. */
   private final ColumnReference[] sortColumns;

   /** Sort orders. */
   private final SortOrder[] sortOrders;

   /**
    * Creates a new {@code Sort} physical operator with the given sort columns and sort orders.
    *
    * @param sortColumns
    *           sort column references
    * @param sortOrders
    *           sort orders
    */
   public Sort(final ColumnReference[] sortColumns, final SortOrder[] sortOrders) {
      super(OperatorType.SORT);
      this.sortColumns = sortColumns;
      this.sortOrders = sortOrders;
   }

   /**
    * Creates a new {@code Sort} physical enforcer operator. This variant of the constructor is used by the
    * rule engine to create a template of a sort operator. Therefore, all its fields are initialized to
    * {@code null}.
    */
   public Sort() {
      super(OperatorType.SORT);
      this.sortColumns = null;
      this.sortOrders = null;
   }

   /**
    * Returns the sort columns.
    *
    * @return sort columns
    */
   public ColumnReference[] getSortColumns() {
      return this.sortColumns;
   }

   /**
    * Returns the sort orders.
    *
    * @return sort orders
    */
   public SortOrder[] getSortOrders() {
      return this.sortOrders;
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final double cardinality = Math.max(1.0,
            ((LogicalCollectionProperties) inputProperties[0]).getCardinality());
      // Compute CPU cost in terms of comparisons and moves
      final double cpuCost = 2 * cardinality * Math.log(cardinality) / Math.log(2.0)
            * CostModel.CPU_COMP_MOVE.getCost();
      // TODO I/O Cost
      final double ioCost = 0.0;
      return new Cost(ioCost, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      if (DataOrder.ANY.equals(requiredProperties.getOrder())) {
         // The sort enforced will not be used if not sorted output is required
         inputRequiredProperties.set(requiredProperties);
         return true;
      }
      inputRequiredProperties.set(PhysicalProperties.anyPhysicalProperties());
      if (DataOrder.SORTED.equals(requiredProperties.getOrder())) {
         return ((LogicalCollectionProperties) inputLogicalProperties).getSchema()
               .containsKey(requiredProperties.getKey());
      }
      throw new OptimizerError("Invalid data order: " + requiredProperties.getOrder() + ".");
   }
}

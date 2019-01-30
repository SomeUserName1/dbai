/*
 * @(#)MergeJoin.java   1.0   Jun 13, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import minibase.catalog.DataOrder;
import minibase.catalog.SortOrder;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.SchemaSortKey;

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.EquiJoin EquiJoin}
 * logical operator using the merge join algorithm. The merge join requires that both its inputs are sorted on
 * the the join columns.
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
public class MergeJoin extends AbstractPhysicalJoin {

   /**
    * Creates a new {@code MergeJoin} physical operator. The join predicate is given in the form of two sets
    * of the same size. Each set contains a number of references to columns in the system catalog. Columns are
    * assumed to be pair-wise equal, i.e., the join predicate is {@code leftColumns[1] = rightColumns[1] AND}
    * ... {@code AND leftColumns[n] = rightColumns[n]}. If both sets of column reference IDs are empty, a
    * natural join or a cross-product is performed.
    *
    * @param leftColumns
    *           left columns
    * @param rightColumns
    *           right columns
    */
   public MergeJoin(final List<ColumnReference> leftColumns, final List<ColumnReference> rightColumns) {
      super(OperatorType.MERGEJOIN, leftColumns, rightColumns);
   }

   /**
    * Constructs a new {@code MergeJoin} physical operator. This variant of the constructor is used by the
    * rule engine to create a template of a this operator. Therefore, all its fields are initialized to
    * {@code null}.
    */
   public MergeJoin() {
      super(OperatorType.MERGEJOIN);
   }

   @Override
   public PhysicalProperties getPhysicalProperties(final PhysicalProperties... inputProperties) {
      // Merge the left and right list of column references into the sort key
      final List<ColumnReference> sortColumns = new ArrayList<>();
      sortColumns.addAll(this.getLeftColumns());
      for (final ColumnReference column : this.getRightColumns()) {
         if (!sortColumns.contains(column)) {
            sortColumns.add(column);
         }
      }
      // Define the sort order.
      final List<SortOrder> sortOrders = Collections.nCopies(sortColumns.size(), SortOrder.ASCENDING);
      // Define the sort key.
      final SchemaSortKey sortKey = new SchemaSortKey(sortColumns, sortOrders);
      return new PhysicalProperties(DataOrder.SORTED, sortKey, null);
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final double leftCardinality = ((LogicalCollectionProperties) inputProperties[0]).getCardinality();
      final double rightCardinality = ((LogicalCollectionProperties) inputProperties[1]).getCardinality();
      final double outputCardinality = ((LogicalCollectionProperties) localProperties).getCardinality();
      // TODO Calculation of I/O cost.
      final double ioCost = 0.0;
      // TODO Cost calculation should also consider worst case.
      final double cpuCost = (leftCardinality + rightCardinality) * CostModel.CPU_PRED.getCost()
            + outputCardinality * CostModel.TOUCH_COPY.getCost();
      return new Cost(ioCost, cpuCost);
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      this.assertInputNo(inputNo);
      // If a specific output order is required, checked whether it matches the output order
      // generated by this merge join.
      if (!DataOrder.ANY.equals(requiredProperties.getOrder())) {
         if (!((SchemaSortKey) requiredProperties.getKey()).isMatch(this.getLeftColumns())
               && !((SchemaSortKey) requiredProperties.getKey()).isMatch(this.getRightColumns())) {
            inputRequiredProperties.set(null);
            return false;
         }
      }
      // Compute the sort key of the output of this merge join.
      final List<ColumnReference> sortColumns;
      if (inputNo == 0) {
         sortColumns = this.getLeftColumns();
      } else {
         sortColumns = this.getRightColumns();
      }
      final List<SortOrder> sortOrders = Collections.nCopies(sortColumns.size(), SortOrder.ASCENDING);
      final SchemaSortKey sortKey = new SchemaSortKey(sortColumns, sortOrders);
      inputRequiredProperties.set(new PhysicalProperties(DataOrder.SORTED, sortKey, null));
      return true;
   }
}

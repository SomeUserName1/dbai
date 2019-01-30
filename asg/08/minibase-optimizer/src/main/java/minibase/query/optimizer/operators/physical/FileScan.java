/*
 * @(#)FileScan.java   1.0   Jun 1, 2014
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
import minibase.catalog.TableDescriptor;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.SchemaSortKey;
import minibase.query.schema.StoredTableReference;

/**
 * Physical operator that implements the {@link minibase.query.optimizer.operators.logical.GetTable GetTable}
 * logical operator as a file scan reads all data from the given file.
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
 */
public class FileScan extends AbstractPhysicalOperator {

   /** Reference to the table. */
   private final StoredTableReference table;

   /** References to the columns. */
   private final List<ColumnReference> columns;

   /**
    * Creates a new file scan physical operator that scans the given table.
    *
    * @param table
    *           table scanned by this operator
    * @param columns
    *           columns scanned by this operator
    */
   public FileScan(final StoredTableReference table, final List<ColumnReference> columns) {
      super(OperatorType.FILESCAN);
      this.table = table;
      this.columns = columns;
   }

   /**
    * Constructs a new {@code FileScan} operator. This variant of the constructor is used by the rule engine
    * to create a template of this operator. Therefore, all its fields are initialized to {@code null}.
    */
   public FileScan() {
      this(null, null);
   }

   /**
    * Returns a reference to the table scanned by this operator.
    *
    * @return table reference
    */
   public StoredTableReference getTable() {
      return this.table;
   }

   @Override
   public PhysicalProperties getPhysicalProperties(final PhysicalProperties... inputProperties) {
      final TableDescriptor table = this.table.getDescriptor();
      if (DataOrder.ANY.equals(table.getDataOrder())) {
         return PhysicalProperties.anyPhysicalProperties();
      } else {
         final SchemaSortKey key = SchemaSortKey.resolve(table.getIndexKey().getKey(), this.columns).get();
         return new PhysicalProperties(table.getDataOrder(), key, null);
      }
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      final LogicalCollectionProperties logicalProperties = (LogicalCollectionProperties) localProperties;
      final double cardinality = logicalProperties.getCardinality();
      final TableDescriptor descriptor = this.table.getDescriptor();
      final double width = descriptor.getStatistics().getWidth();
      final double blocks = Math.ceil(cardinality * width);
      return new Cost(blocks * CostModel.IO.getCost(), blocks * CostModel.CPU_READ.getCost());
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      throw new IllegalStateException("File scan cannot have required input properties.");
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

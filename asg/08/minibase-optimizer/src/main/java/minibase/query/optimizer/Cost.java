/*
 * @(#)Cost.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.List;
import java.util.Locale;

import minibase.catalog.ColumnStatistics;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;
import minibase.query.schema.TableReference;

/**
 * Cost of a physical operator, expression, or multi-expression.
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
public class Cost implements Comparable<Cost> {

   /** Cost value representing a cost of zero. */
   private static final Cost ZERO = new Cost(0, 0);

   /** Cost value representing an infinite cost. */
   private static final Cost INFINITY = new Cost(-1, -1);

   /** Total cost value. */
   private final double totalCost;

   /** I/O cost value. */
   private final double ioCost;

   /** CPU cost value. */
   private final double cpuCost;

   /**
    * Creates a new cost value.
    *
    * @param ioCost
    *           I/O cost value
    * @param cpuCost
    *           CPU cost value
    */
   public Cost(final double ioCost, final double cpuCost) {
      this.ioCost = ioCost;
      this.cpuCost = cpuCost;
      this.totalCost = ioCost + cpuCost;
   }

   /**
    * Returns the I/O cost value.
    *
    * @return I/O cost value
    */
   public double getIO() {
      return this.ioCost;
   }

   /**
    * Returns the CPU cost value.
    *
    * @return CPU cost value
    */
   public double getCPU() {
      return this.cpuCost;
   }

   /**
    * Returns the sum of CPU and I/O cost.
    *
    * @return CPU + I/O
    */
   public double getTotal() {
      return this.totalCost;
   }

   /**
    * Adds the given cost to this cost and returns the result as a new cost.
    *
    * @param cost
    *           cost to add
    * @return result cost
    */
   public Cost plus(final Cost cost) {
      if (this.isInfinity() || cost.isInfinity()) {
         return Cost.INFINITY;
      }
      return new Cost(this.ioCost + cost.ioCost, this.cpuCost + cost.cpuCost);
   }

   /**
    * Subtracts the given cost from this cost and returns the result as a new cost.
    *
    * @param cost
    *           cost to subtract
    * @return result cost
    */
   public Cost minus(final Cost cost) {
      if (this.isInfinity() || cost.isInfinity()) {
         return Cost.INFINITY;
      }
      final double ioCost = this.ioCost - cost.ioCost;
      final double cpuCost = this.cpuCost - cost.cpuCost;
      return new Cost(ioCost > 0 ? ioCost : 0, cpuCost > 0 ? cpuCost : 0);
   }

   /**
    * Divides this cost by the given divisor and returns the result as a new cost.
    *
    * @param divisor
    *           divisor to divide by
    * @return result cost
    */
   public Cost divide(final double divisor) {
      if (this.isInfinity()) {
         return Cost.INFINITY;
      }
      return new Cost(this.ioCost / divisor, this.cpuCost / divisor);
   }

   /**
    * Multiplies this cost with the given factor and returns the result as a new cost.
    *
    * @param factor
    *           factor to multiply with
    * @return result cost
    */
   public Cost multiply(final double factor) {
      if (this.isInfinity()) {
         return Cost.INFINITY;
      }
      return new Cost(this.ioCost * factor, this.cpuCost * factor);
   }

   /**
    * Checks whether this value is zero value.
    *
    * @return {@code true} if this value is zero, {@code false} otherwise
    */
   public boolean isZero() {
      return this.equals(ZERO);
   }

   /**
    * Checks whether this value is the infinity value.
    *
    * @return {@code true} if this value is infinite, {@code false} otherwise
    */
   public boolean isInfinity() {
      return this.equals(INFINITY);
   }

   @Override
   public int compareTo(final Cost other) {
      if (this.totalCost == INFINITY.totalCost && other.totalCost == INFINITY.totalCost) {
         throw new IllegalStateException("Cannot compare two infinite values.");
      }
      if (this.totalCost == INFINITY.totalCost) {
         return 1;
      } else if (other.totalCost == INFINITY.totalCost) {
         return -1;
      } else {
         return Double.compare(this.totalCost, other.totalCost);
      }
   }

   @Override
   public int hashCode() {
      return Double.hashCode(this.totalCost);
   }

   @Override
   public boolean equals(final Object other) {
      if (this == other) {
         return true;
      }
      if (other == null) {
         return false;
      }
      if (this.getClass() != other.getClass()) {
         return false;
      }
      final Cost cost = (Cost) other;
      if (Double.doubleToLongBits(this.totalCost) != Double.doubleToLongBits(cost.totalCost)) {
         return false;
      }
      return true;
   }

   /**
    * Returns a zero cost.
    *
    * @return zero cost
    */
   public static Cost zero() {
      return ZERO;
   }

   /**
    * Returns an infinite cost.
    *
    * @return infinite cost
    */
   public static Cost infinity() {
      return INFINITY;
   }

   /**
    * Computes the total cost from local and input costs.
    *
    * @param localCost
    *           local cost
    * @param inputCosts
    *           input costs
    * @return total cost
    */
   public static Cost totalCost(final Cost localCost, final Cost[] inputCosts) {
      double ioCost = localCost.ioCost;
      double cpuCost = localCost.cpuCost;
      for (final Cost cost : inputCosts) {
         if (cost.isInfinity()) {
            return Cost.INFINITY;
         }
         ioCost += cost.ioCost;
         cpuCost += cost.cpuCost;
      }
      return new Cost(ioCost, cpuCost);
   }

   /**
    * Computes the touch copy costs for the expression with the given logical properties. These costs are used
    * for group pruning.
    *
    * @param properties
    *           logical properties
    * @return touch copy costs
    */
   public static Cost touchCopyCost(final LogicalCollectionProperties properties) {
      // Check whether the cardinality is known.
      double total = properties.getCardinality() == -1 ? 0 : properties.getCardinality();
      double minimum = Double.MAX_VALUE;
      double maximum = 0.0;
      final Schema schema = properties.getSchema();
      for (int i = 0; i < schema.getTableCount(); i++) {
         final TableReference tableRef = schema.getTable(i);
         final double columnUniqueCardinality = getMaximumColumnUniqueCardinality(properties, tableRef);
         minimum = Math.min(minimum, columnUniqueCardinality);
         maximum = Math.max(maximum, columnUniqueCardinality);
         total += columnUniqueCardinality;
      }
      // Exclude minimum and maximum
      total -= minimum;
      total -= maximum;
      final double ioCost = 0.0;
      final double cpuCost = total * CostModel.TOUCH_COPY.getCost();
      return new Cost(ioCost, cpuCost);
   }

   /**
    * Computes the fetching costs for an expression with the given logical properties. These costs are used
    * for column unique pruning.
    *
    * @param properties
    *           logical properties
    * @return fetching costs
    */
   public static Cost fetchingCost(final LogicalCollectionProperties properties) {
      double ioCost = 0.0;
      double cpuCost = 0.0;
      final Schema schema = properties.getSchema();
      for (int i = 0; i < schema.getTableCount(); i++) {
         final TableReference tableRef = schema.getTable(i);
         final double columnUniqueCardinality = getMaximumColumnUniqueCardinality(properties, tableRef);
         final double width = getWidth(properties, tableRef);
         final double blocks = Math.ceil(columnUniqueCardinality * width);

         ioCost += blocks * CostModel.IO.getCost();
         cpuCost += blocks * CostModel.CPU_READ.getCost();
      }
      return new Cost(ioCost, cpuCost);
   }

   /**
    * Returns the maximum column unique cardinality for columns contained in the table identified by the given
    * reference.
    *
    * @param properties
    *           logical properties
    * @param table
    *           table reference
    * @return maximum column unique cardinality
    */
   private static double getMaximumColumnUniqueCardinality(final LogicalCollectionProperties properties,
         final TableReference table) {
      double maximum = 0.0;
      // Iterate over all column statistics in the given logical properties.
      final List<ColumnStatistics> columnStatistics = properties.getColumnStatistics();
      for (int i = 0; i < columnStatistics.size(); i++) {
         final ColumnReference column = properties.getSchema().getColumn(i);
         // Check that the i-th column is contained in the given table.
         if (column.getParent().isPresent() && column.getParent().get().getID() == table.getID()) {
            // Update the maximum column unique cardinality of the given table.
            maximum = Math.max(maximum, columnStatistics.get(i).getUniqueCardinality());
         }
      }
      return maximum;
   }

   /**
    * Returns the average width of tuples as the sum of the widths of all columns contained in this schema
    * that are also contained in the table identified by the given reference.
    *
    * @param properties
    *           logical properties
    * @param table
    *           table reference
    * @return average tuple width
    */
   private static double getWidth(final LogicalCollectionProperties properties, final TableReference table) {
      double width = 0.0;
      // Iterate over all column statistics in the given logical properties.
      final List<ColumnStatistics> columnStatistics = properties.getColumnStatistics();
      for (int i = 0; i < columnStatistics.size(); i++) {
         final ColumnReference column = properties.getSchema().getColumn(i);
         // Check that the i-th column is contained in the given table.
         if (column.getParent().isPresent() && column.getParent().get().getID() == table.getID()) {
            // Add the width of the current column to the total width.
            width += columnStatistics.get(i).getWidth();
         }
      }
      return width;
   }

   @Override
   public String toString() {
      if (this.isInfinity()) {
         return new String("INFINITY");
      }
      return String.format(Locale.US, "%.3f", this.totalCost);
   }
}

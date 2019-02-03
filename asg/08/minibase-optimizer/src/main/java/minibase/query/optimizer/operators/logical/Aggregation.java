/*
 * @(#)Aggregation.java   1.0   Jul 5, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.TableStatistics;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.JenkinsHash;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.DerivedColumnReference;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaForeignKey;
import minibase.query.schema.SchemaKey;
import minibase.query.schema.TableReference;

/**
 * The {@code Aggregation} logical operator applies an aggregate function to its input.
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
 * @author Juergen Hoelsch &lt;juergen.hoelsch@uni.kn&gt;
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class Aggregation extends AbstractLogicalOperator {

   /** Reduction factor of the group-by operation. */
   private static final double REDUCTION_FACTOR = 0.7;

   /** Group-by columns of this aggregation operator. */
   private final List<ColumnReference> groupByColumns;

   /** Columns aggregated by this aggregation operator. */
   private final List<DerivedColumnReference> aggregatedColumns;

   /**
    * Creates a new {@code Aggregation} logical operator with the given group-by and aggregated columns.
    *
    * @param groupByColumns
    *           group-by columns
    * @param aggregatedColumns
    *           aggregated columns
    */
   public Aggregation(final List<ColumnReference> groupByColumns,
         final List<DerivedColumnReference> aggregatedColumns) {
      super(OperatorType.AGGREGATE);
      this.groupByColumns = groupByColumns;
      if (aggregatedColumns == null || aggregatedColumns.size() == 0) {
         throw new IllegalArgumentException("No columns to aggregate provided.");
      }
      this.aggregatedColumns = aggregatedColumns;
   }

   /**
    * Creates a new {@code Aggregation} logical operator.
    */
   public Aggregation() {
      super(OperatorType.AGGREGATE);
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
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      final LogicalCollectionProperties collectionProperties = (LogicalCollectionProperties) inputProperties[0];
      final Schema schema = this.getSchema(collectionProperties.getSchema());
      // is this a group-by/aggregate or just an aggregate operator?
      if (this.groupByColumns == null || this.groupByColumns.size() == 0) {
         final List<ColumnStatistics> statistics = new ArrayList<>();
         double totalWidth = 0.0;
         for (final ColumnReference column : this.aggregatedColumns) {
            final double width = column.getWidth();
            totalWidth += width;
            // if there are no groups, only one value will be returned
            statistics.add(new ColumnStatistics(1, 1, null, null, width));
         }
         // if there are no groups, only one tuple will be returned
         return new LogicalCollectionProperties(schema, new TableStatistics(1, 1, totalWidth), statistics,
               false);
      }
      // gather column statistics of group-by columns
      final List<ColumnStatistics> columnStatistics = new ArrayList<>();
      double totalWidth = 0.0;
      for (final ColumnReference groupByColumn : this.groupByColumns) {
         // look up statistics of this column
         final ColumnStatistics statistics = collectionProperties.getColumnStatistics(groupByColumn);
         columnStatistics.add(statistics);
         totalWidth += statistics.getWidth();
      }
      // compute unique cardinality of group-by operation
      final double cardinality = collectionProperties.getCardinality();
      double uniqueCardinality = 1.0;
      for (final ColumnStatistics statistics : columnStatistics) {
         final double columnUniqueCardinality = statistics.getUniqueCardinality();
         if (columnUniqueCardinality != -1) {
            uniqueCardinality *= columnUniqueCardinality;
         } else {
            uniqueCardinality = REDUCTION_FACTOR * cardinality;
            break;
         }
      }
      uniqueCardinality = Math.min(uniqueCardinality, REDUCTION_FACTOR * cardinality);
      // add widths and column statistics of aggregated columns
      for (final ColumnReference column : this.aggregatedColumns) {
         final double width = column.getWidth();
         totalWidth += width;
         // if there are no groups, only one value will be returned
         columnStatistics.add(new ColumnStatistics(uniqueCardinality, uniqueCardinality, null, null, width));
      }
      return new LogicalCollectionProperties(schema,
            new TableStatistics(uniqueCardinality, uniqueCardinality, totalWidth), columnStatistics, false);
   }

   @Override
   public Schema getSchema(final Schema... inputSchemas) {
      final Schema inputSchema = inputSchemas[0];
      // sanity check to verify that group-by columns are part of the input schema
      for (final ColumnReference column : this.groupByColumns) {
         assert inputSchema.containsColumn(column);
      }
      // build new column list from group-by columns followed by aggregated columns
      final List<ColumnReference> columns = new ArrayList<>();
      columns.addAll(this.groupByColumns);
      columns.addAll(this.aggregatedColumns);
      // build list of table that are contained in the new schema by accessing the column parents
      final List<TableReference> tables = new ArrayList<>();
      for (final ColumnReference column : columns) {
         final Optional<TableReference> parent = column.getParent();
         if (parent.isPresent() && !tables.contains(parent)) {
            tables.add(parent.get());
         }
      }
      // build new candidate keys by taking all existing candidate keys that are fully contained in the
      // group-by columns as well as a new key consisting of all group-by columns
      final Collection<SchemaKey> candidateKeys = new HashSet<>();
      for (final SchemaKey key : inputSchema.getCandidateKeys()) {
         if (key.isSubsetOf(this.groupByColumns)) {
            candidateKeys.add(key);
         }
      }
      candidateKeys.add(new SchemaKey(this.groupByColumns));
      // build new foreign keys by taking all the existing foreign keys that are fully contained in the
      // group-by columns.
      final Collection<SchemaForeignKey> foreignKeys = new HashSet<>();
      for (final SchemaForeignKey foreignKey : inputSchema.getForeignKeys()) {
         if (foreignKey.getReferencingColumns().isSubsetOf(this.groupByColumns)) {
            foreignKeys.add(foreignKey);
         }
      }
      return new Schema(columns, tables, candidateKeys, foreignKeys);
   }

   @Override
   public int hashCode() {
      final int hash = JenkinsHash.lookup2(this.groupByColumns == null ? 0 : this.groupByColumns.hashCode(),
            super.hashCode());
      return JenkinsHash.lookup2(this.aggregatedColumns.hashCode(), hash);
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (this.getClass() != obj.getClass()) {
         return false;
      }
      final Aggregation other = (Aggregation) obj;
      if (this.aggregatedColumns == null) {
         if (other.aggregatedColumns != null) {
            return false;
         }
      } else if (!this.aggregatedColumns.equals(other.aggregatedColumns)) {
         return false;
      }
      if (this.groupByColumns == null) {
         if (other.groupByColumns != null) {
            return false;
         }
      } else if (!this.groupByColumns.equals(other.groupByColumns)) {
         return false;
      }
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

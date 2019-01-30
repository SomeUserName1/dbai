/*
 * @(#)Projection.java   1.0   Jun 13, 2014
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.TableStatistics;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.JenkinsHash;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaForeignKey;
import minibase.query.schema.SchemaKey;
import minibase.query.schema.TableReference;

/**
 * The {@code Projection} logical operator truncates a collection of tuples to the given projection list.
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
public class Projection extends AbstractLogicalOperator {

   /** List of projection columns. */
   private final List<ColumnReference> projectionColumns;

   /**
    * Creates a new {@code Projection} logical operator that projects tuples to the columns in the given
    * projection list.
    *
    * @param projectionColumns
    *           list of projection columns
    */
   @SafeVarargs
   public Projection(final ColumnReference... projectionColumns) {
      this(Arrays.asList(projectionColumns));
   }

   /**
    * Creates a new {@code Projection} logical operator that projects tuples to the columns in the given
    * projection list.
    *
    * @param projectionColumns
    *           list of projection columns
    */
   public Projection(final List<ColumnReference> projectionColumns) {
      super(OperatorType.PROJECT);
      if (projectionColumns != null) {
         this.projectionColumns = projectionColumns;
      } else {
         this.projectionColumns = Collections.emptyList();
      }
   }

   /**
    * Returns the list of projection columns used by this projection operator.
    *
    * @return list of projection columns
    */
   public List<ColumnReference> getProjectionColumns() {
      return this.projectionColumns;
   }

   @Override
   public LogicalCollectionProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      // Get logical collection properties of first and only input.
      final LogicalCollectionProperties collectionProperties = (LogicalCollectionProperties) inputProperties[0];
      // Compute new schema that is created by the projection operator.
      final Schema schema = this.getSchema(collectionProperties.getSchema());
      // Compute set of column statistics of projected columns.
      final List<ColumnStatistics> columnStatistics = new ArrayList<>();
      double width = 0.0;
      for (final ColumnReference column : schema) {
         final ColumnStatistics statistics = collectionProperties.getColumnStatistics(column);
         columnStatistics.add(statistics);
         width += statistics.getWidth();
      }
      // Compute new unique cardinality of the output.
      double uniqueCardinality = 1.0;
      for (final ColumnStatistics statistics : columnStatistics) {
         final double columnUniqueCardinality = statistics.getUniqueCardinality();
         if (columnUniqueCardinality != -1) {
            uniqueCardinality *= columnUniqueCardinality;
         } else {
            uniqueCardinality = collectionProperties.getCardinality();
            break;
         }
      }
      uniqueCardinality = Math.min(uniqueCardinality, collectionProperties.getCardinality());
      final TableStatistics tableStatistics = new TableStatistics(collectionProperties.getCardinality(),
            uniqueCardinality, width);
      return new LogicalCollectionProperties(schema, tableStatistics, columnStatistics,
            collectionProperties.isTable());
   }

   @Override
   public Schema getSchema(final Schema... inputSchemas) {
      return this.project(inputSchemas[0], this.projectionColumns);
   }

   /**
    * Returns a new schema that is the projection of this schema onto the given list of projection columns.
    *
    * @param schema
    *           input schema
    * @param projectionList
    *           projection column list
    * @return projection of this schema onto the given projection list
    */
   public Schema project(final Schema schema, final List<ColumnReference> projectionList) {
      // Compute new candidate key.
      final Set<SchemaKey> candidateKeys = new HashSet<>();
      for (final SchemaKey candidateKey : schema.getCandidateKeys()) {
         if (candidateKey.isSubsetOf(this.projectionColumns)) {
            candidateKeys.add(candidateKey);
         }
      }
      // Compute new foreign keys.
      final List<SchemaForeignKey> foreignKeys = new ArrayList<>();
      for (final SchemaForeignKey foreignKey : schema.getForeignKeys()) {
         if (foreignKey.getReferencingColumns().isSubsetOf(this.projectionColumns)) {
            foreignKeys.add(foreignKey);
         }
      }
      final List<TableReference> tables = new ArrayList<>();
      final List<ColumnReference> columns = new ArrayList<>();
      for (final ColumnReference projectionColumn : projectionList) {
         // Add the columns in the projection list to the new schema and look up their statistics in the
         // original schema.
         for (final ColumnReference schemaColumn : schema) {
            if (projectionColumn.getID() == schemaColumn.getID()) {
               // Projection column instead of schema column is added to support the projection of the same
               // column under different names.
               columns.add(projectionColumn);
            }
         }
         // For each projection column the corresponding parent table is added to the schema, if it is not
         // already present in the schema.
         for (int i = 0; i < schema.getTableCount(); i++) {
            final TableReference schemaTable = schema.getTable(i);
            if (projectionColumn.getParent().isPresent()
                  && projectionColumn.getParent().get().getID() == schemaTable.getID()) {
               if (!tables.contains(schemaTable)) {
                  tables.add(schemaTable);
               }
            }
         }
      }
      return new Schema(columns, tables, candidateKeys, foreignKeys);
   }

   @Override
   public int hashCode() {
      int hashCode = super.hashCode();
      if (this.projectionColumns == null && this.projectionColumns.size() > 0) {
         for (int i = this.projectionColumns.size() - 1; i >= 0; i--) {
            hashCode = JenkinsHash.lookup2(this.projectionColumns.get(i).hashCode(), hashCode);
         }
      }
      return hashCode;
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
      final Projection other = (Projection) obj;
      if (this.projectionColumns == null) {
         if (other.projectionColumns != null) {
            return false;
         }
      } else if (!this.projectionColumns.equals(other.projectionColumns)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      for (int i = 0; i < this.projectionColumns.size(); i++) {
         result.append(this.projectionColumns.get(i).getName());
         if (i + 1 < this.projectionColumns.size()) {
            result.append(", ");
         }
      }
      result.append("]");
      return result.toString();
   }
}

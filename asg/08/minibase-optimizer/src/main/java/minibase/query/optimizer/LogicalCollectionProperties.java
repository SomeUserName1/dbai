/*
 * @(#)LogicalCollectionProperties.java   1.0   Feb 15, 2014
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

import minibase.catalog.ColumnStatistics;
import minibase.catalog.TableStatistics;
import minibase.query.schema.AliasedColumnReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;

/**
 * Logical properties to describe the (collection) result of a logical operator.
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
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public class LogicalCollectionProperties extends LogicalProperties {

   /** Schema of the described collection of tuples. */
   private final Schema schema;

   /** Column statistics of the described collection of tuples. */
   private final List<ColumnStatistics> columnStatistics;

   /** Indicates whether the described collection of tuples directly corresponds to a table. */
   private final boolean table;

   /**
    * Constructs new logical properties for a collection operator based on the given schema and statistics.
    *
    * @param schema
    *           schema of the described collection of tuples
    * @param tableStatistics
    *           table statistics of the described collection of tuples
    * @param columnStatistics
    *           column statistics of the described collection of tuples
    * @param table
    *           {@code true} if the described collection of tuples is a table, {@code false} otherwise
    */
   public LogicalCollectionProperties(final Schema schema, final TableStatistics tableStatistics,
         final List<ColumnStatistics> columnStatistics, final boolean table) {
      super(tableStatistics);
      if (schema.getColumnCount() != columnStatistics.size()) {
         throw new IllegalArgumentException("Number of schema columns " + schema.getColumnCount()
               + " does not match number of column statistics " + columnStatistics.size() + ".");
      }
      this.schema = schema;
      this.columnStatistics = columnStatistics;
      this.table = table;
   }

   /**
    * Returns the schema of the described collection of tuples.
    *
    * @return schema
    */
   public Schema getSchema() {
      return this.schema;
   }

   /**
    * Returns the average width of the tuples in the described collection. The returned value is a statistical
    * value. In order to retrieve the declared width of the tuples in the described collection use
    * {@link LogicalCollectionProperties#getSchema()}.
    *
    * @return average tuple width
    */
   public double getWidth() {
      return this.getStatistics().getWidth();
   }

   @Override
   protected TableStatistics getStatistics() {
      return (TableStatistics) super.getStatistics();
   }

   /**
    * Returns the column statistics of all columns in the described collection of tuples.
    *
    * @return column statistics
    */
   public List<ColumnStatistics> getColumnStatistics() {
      return this.columnStatistics;
   }

   /**
    * Returns the column statistics of the given column.
    *
    * @param column
    *           column reference
    * @return column statistics
    */
   public ColumnStatistics getColumnStatistics(final ColumnReference column) {
      final int index;
      if (column instanceof AliasedColumnReference) {
         index = this.schema.getColumnIndex(((AliasedColumnReference) column).getInputColumns().get(0));
      } else {
         index = this.schema.getColumnIndex(column);
      }
      return this.columnStatistics.get(index);
   }

   /**
    * Checks whether the described collection of tuples directly corresponds to a table.
    *
    * @return {@code true} if the described collection of tuples is a table, {@code false} otherwise
    */
   public boolean isTable() {
      return this.table;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append("[schema=");
      result.append(this.getSchema());
      result.append("]");
      return result.toString();
   }
}

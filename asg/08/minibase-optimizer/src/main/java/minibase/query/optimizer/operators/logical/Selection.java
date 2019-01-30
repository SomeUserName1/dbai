/*
 * @(#)Selection.java   1.0   Jun 6, 2014
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
import java.util.List;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.TableStatistics;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.schema.Schema;
import minibase.query.schema.TableReference;

/**
 * The {@code Selection} logical operator filters a collection of tuples by applying a predicate.
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
public class Selection extends AbstractLogicalOperator {

   /**
    * Creates a new {@code Selection} logical operator.
    */
   public Selection() {
      super(OperatorType.SELECT);
   }

   @Override
   public LogicalCollectionProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      final LogicalCollectionProperties collectionProperties = (LogicalCollectionProperties) inputProperties[0];
      final LogicalElementProperties predicateProperties = (LogicalElementProperties) inputProperties[1];
      // Update global cardinality and unique cardinality by multiplying with the predicate's selectivity.
      final double selectivity = predicateProperties.getSelectivity();
      final double cardinality = Math.ceil(collectionProperties.getCardinality() * selectivity);
      final double uniqueCardinality = Math.ceil(collectionProperties.getUniqueCardinality() * selectivity);
      final double width = collectionProperties.getWidth();
      final Schema schema = this.getSchema(collectionProperties.getSchema());
      final TableStatistics tableStatistics = new TableStatistics(cardinality, uniqueCardinality, width);
      // Initialize the schema of the output tuples of this selection operator. Since the selection operator
      // does not change the tables or columns of the input schema, this method basically copies the input
      // schema into the output schema. However, the selection can affect the statistics of the data.
      final Schema inputSchema = collectionProperties.getSchema();
      // Compute column statistics contained in the input schema.
      final List<ColumnStatistics> columnStatistics = new ArrayList<>();
      for (int i = 0; i < inputSchema.getColumnCount(); i++) {
         final ColumnStatistics inputStatistics = collectionProperties.getColumnStatistics().get(i);
         final double inputColumnUniqueCardinality = inputStatistics.getUniqueCardinality();
         double outputColumnUniqueCardinality = -1;
         // If the column unique cardinality of the current column is known, it is updated accordingly.
         if (inputColumnUniqueCardinality != -1) {
            outputColumnUniqueCardinality = Math.ceil(1 / (1 / inputColumnUniqueCardinality
                  - 1 / collectionProperties.getCardinality() + 1 / cardinality));
         }
         columnStatistics
               .add(new ColumnStatistics(inputStatistics.getCardinality(), outputColumnUniqueCardinality,
                     inputStatistics.getMaximum(), inputStatistics.getMinimum(), inputStatistics.getWidth()));
      }
      // Copy all tables contained in the input schema.
      final List<TableReference> tables = new ArrayList<>();
      for (int i = 0; i < inputSchema.getTableCount(); i++) {
         tables.add(inputSchema.getTable(i));
      }
      // Return the new logical collection properties.
      return new LogicalCollectionProperties(schema, tableStatistics, columnStatistics,
            collectionProperties.isTable());
   }

   @Override
   public Schema getSchema(final Schema... inputSchemas) {
      return inputSchemas[0];
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName();
   }
}

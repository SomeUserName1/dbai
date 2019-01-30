/*
 * @(#)AggregationFunction.java   1.0   Jun 12, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.element;

import java.util.ArrayList;
import java.util.List;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.DataType;
import minibase.query.AggregationType;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.schema.ColumnReference;

/**
 * The {@code AggregationFunction} element operator computes the result of an aggregation function. It is used
 * in derived columns that are created by the {@code Aggregation} logical operator.
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
public class AggregationFunction extends AbstractElementOperator {

   /** Type of this aggregation function. */
   private final AggregationType type;

   /** Arity of this aggregation function. */
   private final int arity;

   /**
    * Creates a new aggregation function operator of the given type.
    *
    * @param type
    *           aggregation type
    * @param arity
    *           arity of the aggregation
    */
   public AggregationFunction(final AggregationType type, final int arity) {
      super(OperatorType.AGGREGATION_FUNCTION);
      this.type = type;
      this.arity = arity;
   }

   /**
    * Returns the aggregation type of this aggregation operator.
    *
    * @return aggregation type
    */
   public AggregationType getAggregationType() {
      return this.type;
   }

   @Override
   public int getArity() {
      return this.arity;
   }

   @Override
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      final ColumnStatistics statistics = ColumnStatistics.emptyStatistics();
      final List<ColumnReference> inputs = new ArrayList<>();
      if (inputProperties == null || inputProperties.length == 0) {
         if (AggregationType.COUNT.equals(this.type)) {
            // Handle COUNT(*)
            final DataType outputType = this.type.getOutputType();
            return new LogicalElementProperties(statistics, outputType, outputType.getSize(), 0.0, inputs);
         }
         throw new IllegalStateException("Aggregation function must have inputs.");
      }
      final DataType[] inputTypes = new DataType[inputProperties.length];
      int size = 0;
      double cardinality = 0.0;
      for (int i = 0; i < inputProperties.length; i++) {
         final LogicalElementProperties properties = (LogicalElementProperties) inputProperties[i];
         inputs.addAll(properties.getInputColumns());
         inputTypes[i] = properties.getType();
         size = Math.max(size, properties.getSize());
         cardinality = Math.max(cardinality, properties.getCardinality());
      }
      // Determine the output type of this aggregation function
      final DataType outputType = this.type.getOutputType(inputTypes);
      return new LogicalElementProperties(statistics, outputType, size, 0.0, inputs);
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder(this.type.name());
      if (this.arity == 0) {
         result.append("(*)");
      }
      return result.toString();
   }
}

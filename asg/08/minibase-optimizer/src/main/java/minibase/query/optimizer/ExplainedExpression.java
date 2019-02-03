/*
 * @(#)ExplainedExpression.java   1.0   Jun 6, 2014
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

import minibase.query.optimizer.operators.Operator;

/**
 * An optimized query that contains optimizer metadata such a the estimated costs and statistics is
 * represented as a tree of {@code ExplainedExpressions}.
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
public class ExplainedExpression extends Expression {

   /** Estimated cost of this expression, including its subexpressions. */
   private final Cost cost;

   /** Estimated cardinality of this expression. */
   private final double cardinality;

   /** Estimated unique cardinality of this expression. */
   private final double uniqueCardinality;

   /** Estimated size of the tuples. */
   private final double width;

   /**
    * Creates a new explained expression that uses the given operator to process the given inputs. The number
    * of inputs given has to match the arity of the operator. Additionally, the estimated cost (including its
    * subexpressions) and cardinality of the expression are given.
    *
    * @param operator
    *           operator used in the expression
    * @param inputs
    *           inputs to the operator
    * @param cost
    *           estimated cost of this expression, including its subexpressions
    * @param cardinality
    *           estimated cardinality of this expression
    * @param uniqueCardinality
    *           estimated cardinality of this expression
    * @param width
    *           estimated average tuple width of this expression
    */
   public ExplainedExpression(final Operator operator, final ExplainedExpression[] inputs, final Cost cost,
         final double cardinality, final double uniqueCardinality, final double width) {
      super(operator, inputs);
      this.cost = cost;
      this.cardinality = cardinality;
      this.uniqueCardinality = uniqueCardinality;
      this.width = width;
   }

   /**
    * Creates a new explained expression that uses the given operator to process the given inputs. The number
    * of inputs given has to match the arity of the operator. Additionally, the estimated cost (including its
    * subexpressions) and cardinality of the expression are given.
    *
    * @param operator
    *           operator used in the expression
    * @param cost
    *           estimated cost of this expression, including its subexpressions
    * @param localProperties
    *           logical properties of the operator
    * @param inputs
    *           inputs to the operator
    */
   public ExplainedExpression(final Operator operator, final Cost cost,
         final LogicalProperties localProperties, final ExplainedExpression... inputs) {
      super(operator, inputs);
      this.cost = cost;
      this.uniqueCardinality = localProperties.getStatistics().getUniqueCardinality();
      if (localProperties instanceof LogicalCollectionProperties) {
         final LogicalCollectionProperties collectionProperties = (LogicalCollectionProperties) localProperties;
         this.cardinality = collectionProperties.getCardinality();
         this.width = collectionProperties.getWidth();
      } else {
         this.cardinality = Double.NaN;
         this.width = Double.NaN;
      }
   }

   /**
    * Returns the estimated cost of this expression, including its subexpressions.
    *
    * @return estimated cost
    */
   public Cost getCost() {
      return this.cost;
   }

   /**
    * Returns the estimated cardinality of this expression.
    *
    * @return estimated cardinality
    */
   public double getCardinality() {
      return this.cardinality;
   }

   /**
    * Returns the estimated unique cardinality of this expression.
    *
    * @return estimated unique cardinality
    */
   public double getUniqueCardinality() {
      return this.uniqueCardinality;
   }

   /**
    * Returns an unmodifiable list of the expression children.
    *
    * @return an unmodifiable list of the expression children
    */
   @Override
   @SuppressWarnings("unchecked")
   public List<ExplainedExpression> getInputs() {
      return (List<ExplainedExpression>) super.getInputs();
   }

   /**
    * Returns the estimated tuple size of this expression.
    *
    * @return estimated tuple size
    */
   public double getWidth() {
      return this.width;
   }
}

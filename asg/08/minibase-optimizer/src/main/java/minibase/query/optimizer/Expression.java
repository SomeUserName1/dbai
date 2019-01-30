/*
 * @(#)Expression.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.operators.logical.LogicalOperator;
import minibase.query.schema.Schema;

/**
 * A query is represented as a recursive tree of {@code Expression} instances that each apply an operator to
 * one or more input expressions to compute a result.
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
public class Expression {

   /** Operator used in this expression. */
   private final Operator operator;

   /** Inputs to the operator. */
   private List<Expression> inputs;

   /** Schema of this expression. */
   private Schema schema;

   /**
    * Creates a new expression that uses the given operator to process the given inputs. The number of inputs
    * given has to match the arity of the operator.
    *
    * @param operator
    *           operator used in the expression
    * @param inputs
    *           inputs to the operator
    */
   public Expression(final Operator operator, final Expression... inputs) {
      this(operator, Arrays.asList(inputs));
   }

   /**
    * Creates a new expression that uses the given operator to process the given inputs. The number of inputs
    * given has to match the arity of the operator.
    *
    * @param operator
    *           operator used in the expression
    * @param inputs
    *           inputs to the operator
    */
   public Expression(final Operator operator, final List<Expression> inputs) {
      this.operator = operator;
      if (inputs != null) {
         if (operator.getArity() == inputs.size()) {
            this.inputs = inputs;
         } else {
            throw new IllegalArgumentException("The number of inputs (" + inputs.size()
                  + ") does not match the arity (" + operator.getArity() + ") of the operator.");
         }
      } else {
         if (operator.getArity() == 0) {
            this.inputs = new ArrayList<>();
         } else {
            throw new IllegalArgumentException(
                  "No inputs given for an operator with a non-zero arity (" + operator.getArity() + ").");
         }
      }
      this.schema = null;
   }

   /**
    * Returns the operator used in this expression.
    *
    * @return operator used in the expression
    */
   public Operator getOperator() {
      return this.operator;
   }

   /**
    * Returns the size of this expression in terms of the number of its inputs.
    *
    * @return expression size;
    */
   public int getSize() {
      return this.inputs.size();
   }

   /**
    * Returns the i<sup>th</sup> input of this expression.
    *
    * @param index
    *           index of the input to return
    * @return i<sup>th</sup> input
    */
   public Expression getInput(final int index) {
      if (index < 0 || index >= this.inputs.size()) {
         throw new IllegalArgumentException("Input index out of range.");
      }
      return this.inputs.get(index);
   }

   /**
    * Returns an unmodifiable list of the expression children.
    *
    * @return an unmodifiable list of the expression children
    */
   public List< ? extends Expression> getInputs() {
      return Collections.unmodifiableList(this.inputs);
   }

   /**
    * Returns the schema of this expression.
    *
    * @return expression schema
    */
   public Schema getSchema() {
      if (this.operator.isLogical()) {
         if (this.schema == null) {
            final Schema[] inputSchemas = new Schema[this.inputs.size()];
            for (int i = 0; i < this.inputs.size(); i++) {
               final Expression expression = this.inputs.get(i);
               if (expression.getOperator().isLogical()) {
                  inputSchemas[i] = expression.getSchema();
               }
            }
            this.schema = ((LogicalOperator) this.operator).getSchema(inputSchemas);
         }
         return this.schema;
      }
      throw new IllegalStateException(
            "Expression with operator " + this.operator.getName() + " has no schema.");
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append(this.operator);
      if (this.operator.getArity() > 0) {
         result.append("(");
         for (int i = 0; i < this.inputs.size(); i++) {
            result.append(this.inputs.get(i).toString());
            if (i + 1 < this.inputs.size()) {
               result.append(", ");
            }
         }
         result.append(")");
      }
      return result.toString();
   }
}

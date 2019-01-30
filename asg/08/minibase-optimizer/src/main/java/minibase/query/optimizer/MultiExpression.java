/*
 * @(#)MultiExpression.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.Arrays;
import java.util.Comparator;

import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.operators.logical.LogicalOperator;
import minibase.query.optimizer.rules.Leaf;
import minibase.query.optimizer.rules.Rule;
import minibase.query.optimizer.util.JenkinsHash;
import minibase.query.optimizer.util.StrongReference;

/**
 * During query optimization, {@code MultiExpression} instances are used to share common expressions. In
 * contrast to {@code Expression} objects, multi-expressions take groups instead of other expressions as
 * input.
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
public class MultiExpression {

   /** Operator of this multi-expression. */
   private final Operator operator;

   /** Inputs to this multi-expression. */
   private final Group[] inputs;

   /** Expression group to which this multi-expression belongs. */
   private final Group group;

   /** Next multi-expression in a linked list of multi-expressions. */
   private MultiExpression next;

   /** Bit-mask to track which rules have already been fired. */
   private int bitmask;

   /**
    * Constructs a new multi-expression with the given operator, inputs, and group.
    *
    * @param operator
    *           operator of this multi-expression
    * @param inputs
    *           inputs to this multi-expression
    * @param group
    *           parent group of new multi-expression
    */
   protected MultiExpression(final Operator operator, final Group[] inputs, final Group group) {
      this.operator = operator;
      this.inputs = inputs;
      this.group = group;
      this.bitmask = 0;
   }

   /**
    * Constructs a new multi-expression by converting the given expression and setting the given group as
    * parent group. Note that the new multi-expression is not added to the search space by adding it to the
    * given group. Inputs of the expression will also be converted and added to the search space. Groups will
    * be created for the converted inputs as needed.
    *
    * @param space
    *           search space in which the multi-expression is inserted
    * @param expression
    *           original expression
    * @param group
    *           parent group of the converted expression
    */
   public MultiExpression(final SearchSpace space, final Expression expression, final Group group) {
      this.operator = expression.getOperator();
      this.inputs = new Group[this.operator.getArity()];
      for (int i = 0; i < this.inputs.length; i++) {
         final Expression input = expression.getInput(i);
         if (input.getOperator().isLeaf()) {
            // leaf operators share the existing group
            this.inputs[i] = ((Leaf) input.getOperator()).getGroup();
         } else {
            // create a new group or try to reuse an existing group
            final StrongReference<Group> groupRef = new StrongReference<>();
            space.insert(input, groupRef);
            this.inputs[i] = groupRef.get();
         }
      }
      this.group = group;
      this.bitmask = 0;
   }

   /**
    * Returns the operator used in this multi-expression.
    *
    * @return operator used in the multi-expression
    */
   public Operator getOperator() {
      return this.operator;
   }

   /**
    * Returns the i<sup>th</sup> input of this multi-expression.
    *
    * @param index
    *           index of the input to return
    * @return i<sup>th</sup> input
    */
   public Group getInput(final int index) {
      return this.inputs[index];
   }

   /**
    * Sets the next multi-expression in the list of multi-expressions.
    *
    * @param next
    *           next multi-expression
    */
   public void setNext(final MultiExpression next) {
      this.next = next;
   }

   /**
    * Returns the next multi-expression.
    *
    * @return next multi-expression
    */
   public MultiExpression getNext() {
      return this.next;
   }

   /**
    * Returns the expression group to which this multi-expression belongs.
    *
    * @return expression group
    */
   public Group getGroup() {
      return this.group;
   }

   /**
    * Returns the bit-mask of this expression that indicates which rules have been fired already.
    *
    * @return bit-mask of this expression
    */
   public int getBitmask() {
      return this.bitmask;
   }

   /**
    * Returns true if the given rule can be fired on this multi-expression. This method can be used to limit
    * the number of times a particular rule can be fired on an expression to one.
    *
    * @param rule
    *           optimizer rule
    * @return {@code true} if the rule can be fired, {@code false} otherwise
    */
   public boolean canFire(final Rule rule) {
      return (this.bitmask & 1 << rule.getIndex()) == 0;
   }

   /**
    * Updates the rule mask to mark the given rule as fired on this multi-expression.
    *
    * @param rule
    *           optimizer rule
    */
   public void setRuleMask(final Rule rule) {
      this.bitmask |= rule.getBitmask();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int hashCode() {
      int hashCode = this.operator.hashCode();
      for (int i = this.inputs.length; i > 0; i--) {
         hashCode = JenkinsHash.lookup2(this.inputs[i - 1].hashCode(), hashCode);
      }
      return hashCode;
   }

   /**
    * Returns a hash code value for the object, see {@link Object#equals(Object)}. Optionally, the hash code
    * value can be computed without taking the order of the operator's input into account.
    *
    * @param ignoreInputOrder
    *           indicates if the hash code value take the order of the operator's inputs into account
    * @return hash code value for this object
    */
   public int hashCode(final boolean ignoreInputOrder) {
      if (ignoreInputOrder) {
         int hashCode = ((LogicalOperator) this.operator).hashCode(true);
         // Bring inputs into canonical
         final Group[] groups = new Group[this.inputs.length];
         System.arraycopy(this.inputs, 0, groups, 0, this.inputs.length);
         Arrays.sort(groups, Comparator.comparingInt(g -> g.getID()));
         for (final Group group : groups) {
            hashCode = JenkinsHash.lookup2(group.hashCode(), hashCode);
         }
         return hashCode;
      }
      return this.hashCode();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean equals(final Object other) {
      return this.equals(other, false);
   }

   /**
    * Checks whether the given object is equal to this one, see {@link Object#equals(Object)}. Optionally, the
    * comparison can be compared without taking the order of the operator's input into account.
    *
    * @param other
    *           reference to object with which to compare
    * @param ignoreInputOrder
    *           indicates if the comparison will take the order of the operator's inputs into account
    * @return {@code true} if the two objects are equal, {@code false} otherwise
    */
   public boolean equals(final Object other, final boolean ignoreInputOrder) {
      if (this == other) {
         return true;
      }
      if (other == null) {
         return false;
      }
      if (this.getClass() != other.getClass()) {
         return false;
      }
      final MultiExpression expression = (MultiExpression) other;
      if (ignoreInputOrder) {
         // Input order can only be ignored for logical expressions.
         if (this.operator.equals(expression.getOperator(), ignoreInputOrder)) {
            if (this.inputs.length != expression.inputs.length) {
               throw new OptimizerError(String.format("Arities do not match for %s and %s: %d != %d.",
                     this.operator.getName(), expression.operator.getName(), this.inputs.length,
                     expression.inputs.length));
            }

            for (final Group leftGroup : this.inputs) {
               boolean found = false;
               for (final Group rightGroup : expression.inputs) {
                  if (leftGroup.equals(rightGroup)) {
                     found = true;
                  }
               }
               if (!found) {
                  return false;
               }
            }
            return true;
         }
      } else {
         if (this.operator.equals(expression.operator)) {
            if (this.inputs.length == expression.inputs.length) {
               for (int i = 0; i < this.inputs.length; i++) {
                  if (!this.inputs[i].equals(expression.inputs[i])) {
                     return false;
                  }
               }
               return true;
            }
         }
      }
      return false;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder(this.operator.toString());
      if (this.group != null) {
         result.append("#").append(this.group.getID());
      }
      if (this.inputs.length > 0) {
         result.append("(");
         for (int i = 0; i < this.inputs.length; i++) {
            result.append("[");
            result.append(this.inputs[i].getID());
            result.append("]");
            if (i + 1 < this.inputs.length) {
               result.append(", ");
            }
         }
         result.append(")");
      }
      return result.toString();
   }
}

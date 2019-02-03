/*
 * @(#)AbstractRule.java   1.0   May 9, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import java.util.Arrays;
import java.util.Iterator;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.ExpressionIteratorFactory;
import minibase.query.optimizer.ExpressionIteratorFactory.Traversal;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.Operator;

/**
 * Abstract implementation of interface {@link Rule} and common super-class of all rules.
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
 * @version 1.0
 */
public abstract class AbstractRule implements Rule {

   /** Rule type. */
   private final RuleType type;

   /** Original pattern to match. */
   private final Expression original;

   /** Substitute pattern that replaces original pattern. */
   private final Expression substitute;

   /** Bit-mask of this rule. */
   private final int bitmask;

   /**
    * Constructs a new rule with the given type, bit-mask, original, and substitute expression.
    *
    * @param type
    *           rule type
    * @param bitmask
    *           bit-mask
    * @param original
    *           original pattern
    * @param substitute
    *           substitute pattern
    */
   protected AbstractRule(final RuleType type, final int bitmask, final Expression original,
         final Expression substitute) {
      this.type = type;
      this.bitmask = bitmask;
      this.original = original;
      this.substitute = substitute;
   }

   /**
    * Constructs a new rule with the given type, original, and substitute expression.
    *
    * @param type
    *           rule type
    * @param original
    *           original pattern
    * @param substitute
    *           substitute pattern
    */
   protected AbstractRule(final RuleType type, final Expression original, final Expression substitute) {
      this(type, 1 << type.ordinal(), original, substitute);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String getName() {
      return this.type.name();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getArity() {
      return this.type.getArity();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getIndex() {
      return this.type.ordinal();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Expression getOriginal() {
      return this.original;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Expression getSubstitute() {
      return this.substitute;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Promise getPromise(final Operator operator, final SearchContext context) {
      return this.substitute.getOperator().isPhysical() ? Promise.PHYSCIAL : Promise.LOGICAL;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isApplicable(final Expression before, final MultiExpression expression,
         final SearchContext context) {
      return true;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isConsistent() {
      try {
         this.check();
      } catch (final IllegalStateException e) {
         System.out.println(e.getMessage());
         return false;
      }
      return true;
   }

   /**
    * Checks the consistency of this rule and helps to debug it by throwing {@link IllegalStateException}s
    * that describe eventual problems. The conditions that are checked are described in the comment of method
    * {@link Rule#isConsistent()}.
    */
   protected void check() {
      final boolean[] indexes = new boolean[this.getArity()];
      Arrays.fill(indexes, false);
      Iterator<Expression> iterator = ExpressionIteratorFactory
            .getIterator(this.original, Traversal.PREORDER);
      while (iterator.hasNext()) {
         final Operator operator = iterator.next().getOperator();
         if (operator.isLeaf()) {
            final int index = ((Leaf) operator).getIndex();
            if (index >= 0 && index < this.getArity()) {
               if (!indexes[index]) {
                  indexes[index] = true;
               } else {
                  throw new IllegalStateException("Index " + index + " is already used.");
               }
            } else {
               throw new IllegalStateException("Index " + index + " is out of bounds [0, "
                     + (this.getArity() - 1) + "].");
            }
         } else if (!operator.isLogical()) {
            throw new IllegalStateException("Original pattern can only contain logical operators: "
                  + operator.getName() + ".");
         }
      }
      for (int i = 0; i < indexes.length; i++) {
         if (!indexes[i]) {
            throw new IllegalStateException("Index " + i + " is not used in original pattern.");
         }
      }
      boolean root = true;
      iterator = ExpressionIteratorFactory.getIterator(this.substitute, Traversal.PREORDER);
      while (iterator.hasNext()) {
         final Operator operator = iterator.next().getOperator();
         if (operator.isLeaf()) {
            final int index = ((Leaf) operator).getIndex();
            if (index < 0 || index > this.getArity() - 1 || !indexes[index]) {
               throw new IllegalStateException("Invalid index " + index + " found in substitute pattern.");
            }
         } else if (operator.isPhysical()) {
            if (!root) {
               throw new IllegalStateException(
                     "Only the root of the substitute pattern can be a physical opertor: "
                           + operator.getName() + ".");
            }
         } else if (!operator.isLogical()) {
            throw new IllegalStateException("Operator with invalid type found in subsitution pattern: "
                  + operator.getName() + ".");
         }
         root = false;
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isRootMatch(final MultiExpression expression) {
      final Operator root = expression.getOperator();
      if (!root.isLogical()) {
         throw new IllegalStateException("Root operator " + root + " is not a logical operator.");
      }
      if (this.original.getOperator().isLeaf()) {
         // The original represents a group, i.e., is a leaf operator, which always matches
         return true;
      }
      // Otherwise, the two root operators match if they have the same type
      return this.original.getOperator().getType().equals(expression.getOperator().getType());
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isLogicalToLogical() {
      return this.substitute.getOperator().isLogical();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isLogicalToPhysical() {
      return this.substitute.getOperator().isPhysical();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getBitmask() {
      return this.bitmask;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      return this.getName();
   }
}

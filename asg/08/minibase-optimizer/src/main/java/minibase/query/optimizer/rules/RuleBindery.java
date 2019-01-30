/*
 * @(#)RuleBindery.java   1.0   May 10, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import java.util.Iterator;
import java.util.NoSuchElementException;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.operators.Operator;

/**
 * The rule bindery is used by the rule engine of the optimizer to bind pattern-based rules to expressions and
 * to iterate over all possible bindings.
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
public class RuleBindery implements Iterator<Expression> {

   /** State of this bindery. */
   private State state = State.START;

   /** Group ID of the current expression. */
   private final Group group;

   /** Multi-expression to which the original pattern is bound. */
   private MultiExpression current;

   /** Original pattern that is bound to an expression or a group. */
   private final Expression original;

   /** List of rule binderies for input expressions. */
   private RuleBindery[] inputs = null;

   /** Flag to indicate whether this bindery binds to one expression. */
   private final boolean singleExpression;

   /**
    * Constructs a new rule bindery for the given group.
    *
    * @param group
    *           group to bind to
    * @param original
    *           original pattern from rule
    */
   public RuleBindery(final Group group, final Expression original) {
      this.group = group;
      this.current = null;
      this.original = original;
      this.singleExpression = false;
   }

   /**
    * Constructs a new rule bindery for the given expression.
    *
    * @param expression
    *           expression to bind to
    * @param original
    *           original pattern from rule
    */
   public RuleBindery(final MultiExpression expression, final Expression original) {
      this.group = expression.getGroup();
      this.current = expression;
      this.original = original;
      this.singleExpression = true;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean hasNext() {
      return this.advance();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Expression next() {
      return this.extract();
   }

   /**
    * Extracts and returns the current binding expression.
    *
    * @return binding expression
    */
   private Expression extract() {
      final Operator patternOperator = this.original.getOperator();

      if (this.state == State.VALID_BINDING || this.state == State.FINISHED || patternOperator.isLeaf()
            && this.state == State.START) {

         Expression result = null;
         if (patternOperator.isLeaf()) {
            // create a new leaf operator that is bound to the group of this bindery
            result = new Expression(new Leaf(((Leaf) patternOperator).getIndex(), this.group));
         } else {
            final Operator operator = this.current.getOperator();
            if (operator.getArity() > 0) {
               // expression has sub-expressions
               final Expression[] subExpressions = new Expression[operator.getArity()];
               for (int i = 0; i < operator.getArity(); i++) {
                  // get bound expression for each sub-expression
                  subExpressions[i] = this.inputs[i].extract();
               }
               result = new Expression(operator, subExpressions);
            } else {
               // expression has no sub-expressions
               result = new Expression(operator);
            }
         }
         return result;
      } else {
         throw new NoSuchElementException();
      }
   }

   /**
    * Walks the trees embedded in the search space structure in order to find possible bindings. The walking
    * is done according to the finite state machine described by Yongwen Xu. The method returns
    * {@code code} if a binding is available and {@code false} otherwise.
    *
    * @return {@code true} if a binding was found, {@code false} otherwise
    */
   private boolean advance() {
      final Operator patternOperator = this.original.getOperator();
      if (patternOperator.isLeaf()) {
         switch (this.state) {
            case START:
               this.state = State.FINISHED;
               return true;
            case FINISHED:
               return false;
            case VALID_BINDING:
            default:
               throw new IllegalStateException("Illegal bindery state: " + this.state + ".");
         }
      }
      if (!this.singleExpression && this.state == State.START) {
         this.current = this.group.getFirstLogicalExpression();
      }
      // loop until a new binding is found or no more bindings are possible
      while (true) {
         final Operator operator = this.current.getOperator();

         if (!operator.isLogical()) {
            throw new OptimizerError("Expected logical operator.");
         }

         // analyze state and perform corresponding transition
         switch (this.state) {
            case START:
               if (operator.getArity() != patternOperator.getArity()
                     || operator.getType() != patternOperator.getType()) {
                  // operator and pattern operator do not match: try the next expression
                  this.state = State.FINISHED;
                  break;
               }
               // operator and pattern operator match
               if (operator.getArity() == 0) {
                  // operator matches and has no input expressions
                  this.state = State.VALID_BINDING;
                  return true;
               } else {
                  // operator matches, but has input expressions: create a new bindery for each input
                  // expression
                  this.inputs = new RuleBindery[operator.getArity()];
                  for (int i = 0; i < operator.getArity(); i++) {
                     this.inputs[i] = new RuleBindery(this.current.getInput(i), this.original.getInput(i));
                     // try to advance the new bindery to a binding, a failure to do so is a failure for the
                     // entire expression
                     if (this.inputs[i].advance()) {
                        if (i + 1 == operator.getArity()) {
                           // all binderies have successfully been advanced
                           this.state = State.VALID_BINDING;
                           return true;
                        }
                     } else {
                        this.inputs = null;
                        this.state = State.FINISHED;
                        break;
                     }
                  }
                  break;
               }
            case VALID_BINDING:
               // try to advance input binderies from right to left, the first success is an overall success
               for (int i = operator.getArity() - 1; i >= 0; i--) {
                  if (this.inputs[i].advance()) {
                     // found one more binding, new binderies for all inputs right of the current input have
                     // to be created in order to get all possible bindings. This is inefficient code since
                     // each input on the right has multiple binderies created for it, and each bindery
                     // produces the same bindings as the others. The simplest example of this behavior is the
                     // exchange rule.
                     // TODO Cache rule binderies in order to make this code more efficient
                     for (int j = i + 1; j < operator.getArity(); j++) {
                        this.inputs[j] = new RuleBindery(this.current.getInput(j), this.original.getInput(j));
                        if (!this.inputs[j].advance()) {
                           throw new IllegalStateException("Illegal bindery state: " + this.state + ".");
                        }
                     }
                     // report overall success
                     this.state = State.VALID_BINDING;
                     return true;
                  }
               }
               // there are no more bindings for this logical expression
               this.inputs = null;
               this.state = State.FINISHED;
               break;
            case FINISHED:
               if (this.singleExpression) {
                  return false;
               }
               this.current = this.current.getNext();
               if (this.current == null) {
                  return false;
               } else {
                  this.state = State.START;
                  break;
               }
            default:
               throw new IllegalStateException("Illegal bindery state: " + this.state + ".");
         }
      }
   }

   /**
    * Enumeration that describes the different states in which the bindery can be during the generation of
    * binding the rule pattern to the expression or group.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private enum State {
      /** State of the bindery at the beginning, i.e., new group or expression. */
      START,
      /** State to indicate that a valid binding has been found. */
      VALID_BINDING,
      /** State of the bindery at the end, i.e., no more possible bindings. */
      FINISHED;
   }
}

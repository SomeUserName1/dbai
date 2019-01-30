/*
 * @(#)OptimizeExpressionTask.java   1.0   May 24, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import minibase.query.optimizer.CascadesQueryOptimizer;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.SearchSpace;
import minibase.query.optimizer.Winner;
import minibase.query.optimizer.rules.Promise;
import minibase.query.optimizer.rules.Rule;

/**
 * Task that optimizes a given multi-expression by firing all relevant rules for this multi-expression.
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
public final class OptimizeExpressionTask extends AbstractOptimizerTask {

   /** Multi-expression to optimize. */
   private final MultiExpression expression;

   /**
    * Creates a new optimizer task that optimizes the given group. Setting {@code explore} to {@code true}
    * should not happen, see {@link ExploreGroupTask}.
    *
    * @param optimizer
    *           Cascades optimizer that scheduled this task
    * @param context
    *           search context used by this task
    * @param parent
    *           parent task
    * @param explore
    *           {@code true} if this task is for exploring, {@code false} otherwise
    * @param last
    *           {@code true} if this is the last task for this group, {@code false} otherwise
    * @param bound
    *           bound for global epsilon pruning, {@code null} if not global epsilon pruning is to be
    *           performed
    * @param expression
    *           expression to optimize
    */
   public OptimizeExpressionTask(final CascadesQueryOptimizer optimizer, final SearchContext context,
         final OptimizerTask parent, final boolean explore, final boolean last, final Cost bound,
         final MultiExpression expression) {
      super(optimizer, context, parent, explore, last, bound);
      this.expression = expression;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void perform(final SearchSpace searchSpace) {
      if (this.isExploring() && !this.expression.getOperator().isLogical()) {
         throw new IllegalStateException("Only logical expressions can be explored.");
      }
      if (this.expression.getOperator().isElement()) {
         // push tasks to optimize the inputs of the item operator
         this.getOptimizer().addOptimizerTasks(
               new OptimizeInputsTask(this.getOptimizer(), this.getSearchContext(), this, this.isLast(), this
                     .getBound(), this.expression));
      } else {
         final List<Move> moves = this.getMoves();
         // Sort moves according to promise
         Collections.sort(moves);
         // Optimize expression by applying rules in order of promise
         for (final Move move : moves) {
            // Since new child tasks will be added based on rule application, this task will not be the last
            // task in the sequence of optimizer tasks anymore.
            boolean childLast = false;
            if (this.isLast()) {
               this.setLast(false);
               childLast = true;
            }
            final Rule rule = move.getRule();
            this.getOptimizer().addOptimizerTasks(
                  new ApplyRuleTask(this.getOptimizer(), this.getSearchContext(), this, this.isExploring(),
                        childLast, this.getBound(), rule, this.expression));
            // If the current rule is an enforcer rule, do not explore patterns
            final Expression original = rule.getOriginal();
            if (original.getOperator().isLeaf()) {
               continue;
            }
            // Explore all inputs to match the original pattern for earlier tasks
            for (int i = original.getSize(); i > 0; i--) {
               final int inputNo = i - 1;
               // Only explore inputs that have inputs themselves, i.e., where arity > 0
               if (original.getInput(inputNo).getSize() > 0) {
                  // It the input group has not yet been explored, schedule a task to explore it under the
                  // current context
                  final Group group = this.expression.getInput(inputNo);
                  // TODO isExploring() or isExplored()
                  if (!group.isExplored()) {
                     // The new optimizer task to explore the group cannot be the last task for this group
                     this.getOptimizer().addOptimizerTasks(
                           new ExploreGroupTask(this.getOptimizer(), this.getSearchContext(), this
                                 .getParent(), false, this.getBound(), group));
                  }
               }
            }
         }
      }
      this.updateGroup(this.expression.getGroup());
   }

   /**
    * If this task is still the last optimizer task for this group, the group will be marked as completed in
    * terms of exploration or optimization.
    *
    * @param group
    *           expression group
    */
   private void updateGroup(final Group group) {
      if (this.isLast()) {
         // This is still the last optimizer task for the group
         if (this.isExploring()) {
            group.setExplored(true);
         } else {
            // TODO Optimize group for all interesting relevant properties (directive IRPROP).
            final Winner winner = group.getWinner(this.getSearchContext().getRequiredProperties());
            if (winner.isReady()) {
               throw new OptimizerError("Winner is already optimized.");
            }
            winner.setReady(true);
            group.setOptimized(true);
         }
      }
   }

   /**
    * Returns a list of possible moves, i.e., rules that can be applied to the expression that is being
    * optimized.
    *
    * @return moves list
    */
   private List<Move> getMoves() {
      final List<Move> moves = new ArrayList<>();
      for (final Rule rule : this.getOptimizer().getRuleManager().getActiveRules()) {
         // TODO Make configurable (directive UNIQ).
         if (!this.expression.canFire(rule)) {
            // Rule has already been fired
            continue;
         }
         if (this.isExploring() && rule.getSubstitute().getOperator().isPhysical()) {
            // Only fire transformation, i.e., not implementation rules, when exploring
            continue;
         }
         // Determine the promise of the rule based on the root operator of the expression that is optimized
         // and the current context
         if (rule.isRootMatch(this.expression)) {
            final Promise promise = rule.getPromise(this.expression.getOperator(), this.getSearchContext());
            if (promise.getValue() > Promise.NONE.getValue()) {
               // Add a new move to the list of moves
               moves.add(new Move(rule, promise));
            }
         }
      }
      return moves;
   }

   /**
    * Private class to represent a move, i.e., a rule together with its promise.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private final class Move implements Comparable<Move> {

      /** Rule applied in this move. */
      private final Rule rule;

      /** Promise value of this move, which is the promise value of the applied rule. */
      private final Promise promise;

      /**
       * Creates a new move for the given rule and with the given promise.
       *
       * @param rule
       *           applied rule
       * @param promise
       *           promise value.
       */
      private Move(final Rule rule, final Promise promise) {
         this.rule = rule;
         this.promise = promise;
      }

      /**
       * Returns the rule applied in this move.
       *
       * @return applied rule
       */
      private Rule getRule() {
         return this.rule;
      }

      /**
       * Returns the promise value of this move, which is the promise value of the applied rule.
       *
       * @return promise value
       */
      private Promise getPromise() {
         return this.promise;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public int compareTo(final Move other) {
         if (this.getPromise().getValue() > other.getPromise().getValue()) {
            return 1;
         } else if (this.getPromise().getValue() < other.getPromise().getValue()) {
            return -1;
         } else {
            return 0;
         }
      }

      @Override
      public String toString() {
         final StringBuffer result = new StringBuffer("(");
         result.append(this.rule.getName());
         result.append(", ");
         result.append(this.promise.getValue());
         result.append(")");
         return result.toString();
      }
   }
}

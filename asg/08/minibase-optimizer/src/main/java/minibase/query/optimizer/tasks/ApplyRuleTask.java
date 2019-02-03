/*
 * @(#)ApplyRuleTask.java   1.0   May 25, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.tasks;

import minibase.query.optimizer.CascadesQueryOptimizer;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.SearchSpace;
import minibase.query.optimizer.Winner;
import minibase.query.optimizer.rules.Rule;
import minibase.query.optimizer.rules.RuleBindery;

/**
 * Optimizer task that applies a single rule to a single multi-expression.
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
public final class ApplyRuleTask extends AbstractOptimizerTask {

   /** Rule to apply. */
   private final Rule rule;

   /** Root of expression before rule application. */
   private final MultiExpression expression;

   /**
    * Creates a new optimizer task to apply the given rule to the given multi-expression.
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
    * @param rule
    *           rule to apply
    * @param expression
    *           root of expression before rule application
    */
   public ApplyRuleTask(final CascadesQueryOptimizer optimizer, final SearchContext context,
         final OptimizerTask parent, final boolean explore, final boolean last, final Cost bound,
         final Rule rule, final MultiExpression expression) {
      super(optimizer, context, parent, explore, last, bound);
      this.rule = rule;
      this.expression = expression;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void perform(final SearchSpace searchSpace) {
      // TODO Verify that this implementation is correct (directive _GEN_LOG).
      // Stop generating logical expressions if the optimization context is finished and (a) epsilon pruning
      // is being applied or (b) the substitute is physical.
      if (this.getSearchContext().isFinished()) {
         if (this.isGlolbalEpsilonPruning() || this.rule.isLogicalToPhysical()) {
            this.updateGroup(this.expression.getGroup());
            return;
         }
      }
      // TODO Make configurable (directive UNIQ).
      if (!this.expression.canFire(this.rule)) {
         // Rule has already been fired
         this.updateGroup(this.expression.getGroup());
         return;
      }
      if (this.getOptimizer().getSettings().isEstimateGlobalEpsilonCost()) {
         this.getOptimizer().getStatistics().ruleFired();
      }

      // System.out.println(this.rule.getName() + " ---> Group[" + this.expression.getGroup().getID() + "]");

      // Rule bindery
      final RuleBindery bindery = new RuleBindery(this.expression, this.rule.getOriginal());
      // Existing expression that is currently bound to the original pattern of the rule
      Expression before;
      // Resulting expression that corresponds to the substitute pattern of the rule
      Expression after;
      // Resulting multi-expression that is included in the search space
      MultiExpression result;

      // TODO Sort possible moves in order of estimated cost (directive SORT_AFTERS).
      // Iterator over all possible binding of the expression optimized by this task
      while (bindery.hasNext()) {
         before = bindery.next();

         // Check the rule's condition
         if (!this.rule.isApplicable(before, this.expression, this.getSearchContext())) {
            // Try to find another binding
            continue;
         }

         // Try to derive a new substitute expression
         final PhysicalProperties requiredProperties = this.getSearchContext().getRequiredProperties();
         after = this.rule.nextSubstitute(before, requiredProperties);
         if (after == null) {
            throw new IllegalStateException("Subsitute expression cannot be null.");
         }

         // Add after expression to search space or not
         final Group group = this.expression.getGroup();
         if (this.getOptimizer().getSettings().isNoPhysicalExpressionsInGroups()) {
            // Do not include physical multi-expressions in the group
            if (after.getOperator().isLogical()) {
               result = searchSpace.insert(after, group);
            } else {
               result = new MultiExpression(searchSpace, after, group);
            }
         } else {
            // Allow inclusion of physical multi-expressions in groups
            result = searchSpace.insert(after, group);
         }

         // Skip the substitute expression if it is already in the search space
         if (result == null) {
            continue;
         }

         // TODO Update optimizer statistics: MemoizedMultiExpressions++.
         result.setRuleMask(this.rule);

         // TODO Handle this case for rules like PROJECT -> NULL by merging groups
         if (this.expression.getGroup().getID() != result.getGroup().getID()) {
            throw new IllegalStateException(
                  "Both original and new expression must be contained in the same group.");
         }

         // Since new child tasks will be added to either explore the new expression or optimize its inputs,
         // this task will not be the last task in the sequence of optimizer tasks anymore.
         boolean childLast = false;
         if (this.isLast()) {
            this.setLast(false);
            childLast = true;
         }

         // Schedule follow-on tasks: if the optimizer is exploring, the new multi-expression must be a
         // logical expression
         if (this.isExploring()) {
            // Optimizer is exploring
            if (!result.getOperator().isLogical()) {
               throw new IllegalStateException("Only logical expressions are valid during exploration.");
            }
            this.getOptimizer().addOptimizerTasks(
                  new OptimizeExpressionTask(this.getOptimizer(), this.getSearchContext(), this, this
                        .isExploring(), childLast, this.getBound(), result));
         } else {
            // Optimizer is optimizing
            if (result.getOperator().isLogical()) {
               // Try further transformation of logical expression
               this.getOptimizer().addOptimizerTasks(
                     new OptimizeExpressionTask(this.getOptimizer(), this.getSearchContext(), this, this
                           .isExploring(), childLast, this.getBound(), result));
            } else {
               // Optimize inputs of physical expression
               if (!result.getOperator().isPhysical()) {
                  throw new IllegalStateException("Unexpected expression type encountered.");
               }
               // TODO Optimize group for all interesting relevant properties (directive IRPROP).
               this.getOptimizer().addOptimizerTasks(
                     new OptimizeInputsTask(this.getOptimizer(), this.getSearchContext(), this, childLast,
                           this.getBound(), result));
            }
         }
      }
      this.expression.setRuleMask(this.rule);
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
}

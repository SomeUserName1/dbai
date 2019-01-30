/*
 * @(#)OptimizeGroupTask.java   1.0   May 25, 2014
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
import java.util.List;

import minibase.catalog.DataOrder;
import minibase.query.optimizer.CascadesQueryOptimizer;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.SearchSpace;
import minibase.query.optimizer.WinnerStatus;
import minibase.query.optimizer.rules.Rule;
import minibase.query.optimizer.rules.RuleType;

/**
 * Task that optimizes a group by finding the cheapest plan in the group that satisfies a context.
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
public final class OptimizeGroupTask extends AbstractOptimizerTask {

   /** Group to optimize. */
   private final Group group;

   /**
    * Creates a new optimizer task that optimizes the given group.
    *
    * @param optimizer
    *           Cascades optimizer that scheduled this task
    * @param context
    *           search context used by this task
    * @param parent
    *           parent task
    * @param last
    *           {@code true} if this is the last task for this group, {@code false} otherwise
    * @param bound
    *           bound for global epsilon pruning, {@code null} if not global epsilon pruning is to be
    *           performed
    * @param group
    *           group to optimize
    */
   public OptimizeGroupTask(final CascadesQueryOptimizer optimizer, final SearchContext context,
         final OptimizerTask parent, final boolean last, final Cost bound, final Group group) {
      super(optimizer, context, parent, false, last, bound);
      this.group = group;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void perform(final SearchSpace searchSpace) {
      final MultiExpression logicalExpression = this.group.getFirstLogicalExpression();
      if (logicalExpression.getOperator().isConstant()) {
         // This is a constant group and can therefore not be optimized.
         return;
      }

      // TODO Optimize group for all interesting relevant properties (directive IRPROP).
      final PhysicalProperties requiredProperties = this.getSearchContext().getRequiredProperties();
      final Cost upperBound = this.getSearchContext().getUpperBound();

      final WinnerStatus status = this.group.getWinnerStatus(this.getSearchContext(), null);

      // Terminate optimization as a winner that matches the search context has already been found
      if (!status.requiresSearch()) {
         return;
      }

      // Check if the group is already optimized
      if (!this.group.isOptimized()) {
         // Group is not yet optimized
         if (DataOrder.ANY.equals(requiredProperties.getOrder())) {
            // Add a winner with a null plan
            this.group.addWinner(null, requiredProperties, upperBound, false);
            // Push optimize expression task for first logical expression
            this.getOptimizer().addOptimizerTasks(
                  new OptimizeExpressionTask(this.getOptimizer(), this.getSearchContext(), this, false, true,
                        this.getBound(), logicalExpression));
         } else {
            // Assert that the data order is sorted
            if (!DataOrder.SORTED.equals(requiredProperties.getOrder())) {
               throw new IllegalStateException("Sorted order expected.");
            }
            // Push optimize group task with current context
            this.getOptimizer().addOptimizerTasks(
                  new OptimizeGroupTask(this.getOptimizer(), this.getSearchContext(), this, true, this
                        .getBound(), this.group));
            // Push optimize group task with any context
            final PhysicalProperties physicalProperties = PhysicalProperties.anyPhysicalProperties();
            final SearchContext context = new SearchContext(physicalProperties, upperBound);
            this.getOptimizer()
                  .addOptimizerTasks(
                        new OptimizeGroupTask(this.getOptimizer(), context, this, true, this.getBound(),
                              this.group));
         }
      } else {
         // Group is already optimized
         if (DataOrder.ANY.equals(requiredProperties.getOrder())) {
            // Assert that this is a case where more search is needed (cf. WinnerStatus.MORESEARCH)
            if (!status.requiresSearch() || !status.foundWinner()) {
               throw new IllegalStateException("Unexpected winner status.");
            }
            this.pushOptimizeInputTasks(this.group.getFirstPhysicalExpression());
         } else {
            this.pushOptimizeInputTasks(this.group.getFirstPhysicalExpression());
            if (!status.foundWinner()) {
               // A specific data order is required, but the appropriate enforcer is not in the group. Push an
               // apply rule task for the enforcer, which cannot be the last task.
               if (DataOrder.SORTED.equals(requiredProperties.getOrder())) {
                  final Rule rule = this.getOptimizer().getRuleManager().getRule(RuleType.SORT_ENFORCER);
                  this.getOptimizer().addOptimizerTasks(
                        new ApplyRuleTask(this.getOptimizer(), this.getSearchContext(), this, false, false,
                              this.getBound(), rule, logicalExpression));
               } else {
                  throw new IllegalStateException("Only sorted data order is currently supported.");
               }
            }
            // Add a winner with a null plan to the winner circle of the group (cf. WinnerStatus.NEWSEARCH)
            if (status.requiresSearch() && !status.foundWinner()) {
               this.group.addWinner(null, requiredProperties, upperBound, false);
            }
         }
      }
   }

   /**
    * Pushes optimize input tasks in reverse order for an expression list that is given in terms of its head.
    *
    * @param headExpression
    *           head of expression list
    */
   private void pushOptimizeInputTasks(final MultiExpression headExpression) {
      // Get all physical multi-expressions from group
      final List<MultiExpression> physicalExpressions = new ArrayList<>();
      MultiExpression physicalExpression = headExpression;
      while (physicalExpression != null) {
         physicalExpressions.add(physicalExpression);
         physicalExpression = physicalExpression.getNext();
      }
      // Push optimize input tasks for physical expressions in reverse order
      boolean last = true;
      for (int i = physicalExpressions.size(); i > 0; i--) {
         this.getOptimizer().addOptimizerTasks(
               new OptimizeInputsTask(this.getOptimizer(), this.getSearchContext(), this, last, this
                     .getBound(), physicalExpressions.get(i - 1)));
         last = false;
      }
   }
}

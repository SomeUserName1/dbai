/*
 * @(#)ExploreGroupTask.java   1.0   May 24, 2014
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
import minibase.query.optimizer.Group;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.SearchSpace;

/**
 * Task that explores a given group by pushing tasks onto the optimizer queue that explore the inputs of the
 * group.
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
public final class ExploreGroupTask extends AbstractOptimizerTask {

   /** Group to explore. */
   private final Group group;

   /**
    * Creates a new optimizer task that explores the given group.
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
    *           group to explore
    */
   public ExploreGroupTask(final CascadesQueryOptimizer optimizer,  final SearchContext context,
         final OptimizerTask parent, final boolean last, final Cost bound,
         final Group group) {
      super(optimizer, context, parent, false, last, bound);
      this.group = group;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void perform(final SearchSpace searchSpace) {
      if (!this.group.isOptimized() && !this.group.isExplored()) {
         if (this.group.isExploring()) {
            throw new IllegalStateException("Group #" + this.group.getID() + " is already being explored.");
         }
         this.group.setExploring(true);
         this.group.setExplored(false);
         // Only the first logical expression is required as it will generate all logical expressions through
         // rule application. At the same time, duplicates are avoided because on the rule bit vector. This
         // logical expression is therefore the last one in the group and marked accordingly.
         final MultiExpression expression = this.group.getFirstLogicalExpression();
         this.getOptimizer().addOptimizerTasks(
               new OptimizeExpressionTask(this.getOptimizer(), this.getSearchContext(),
                     this, true, true, this.getBound(), expression));
      }
   }
}

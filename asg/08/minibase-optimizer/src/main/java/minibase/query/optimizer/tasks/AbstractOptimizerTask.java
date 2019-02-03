/*
 * @(#)AbstractOptimizerTask.java   1.0   May 24, 2014
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
import minibase.query.optimizer.SearchContext;

/**
 * Abstract implementation of an optimizer task that serves as a common super-class for all tasks known to the
 * Minibase optimizer.
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
public abstract class AbstractOptimizerTask implements OptimizerTask {

   /** Cascades optimizer that schedule this task. */
   private final CascadesQueryOptimizer optimizer;

   /** Search context used by this task. */
   private final SearchContext context;

   /** Parent task of this optimizer task. */
   private final OptimizerTask parent;

   /** Indicates whether this task is for exploring. */
   private final boolean explore;

   /** Indicates whether this is the last task for the group. */
   private boolean last;

   /** Epsilon bound for global epsilon pruning. */
   private final Cost bound;

   /**
    * Creates a new optimizer task with the given context ID and the given parent task, i.e., the task that
    * created this optimizer task.
    *
    * @param optimizer
    *           Cascades optimizer that scheduled this task
    * @param context
    *           search context
    * @param parent
    *           parent task
    * @param explore
    *           {@code true} if this task is for exploring, {@code false} otherwise
    * @param last
    *           {@code true} if this is the last task for a group, {@code false} otherwise
    * @param bound
    *           bound for global epsilon pruning, {@code null} if not global epsilon pruning is to be
    *           performed
    */
   protected AbstractOptimizerTask(final CascadesQueryOptimizer optimizer, final SearchContext context,
         final OptimizerTask parent, final boolean explore, final boolean last, final Cost bound) {
      this.optimizer = optimizer;
      this.context = context;
      this.parent = parent;
      this.explore = explore;
      this.last = last;
      this.bound = bound;
   }

   /**
    * Creates a new optimizer task with the given context ID and the given parent task, i.e., the task that
    * created this optimizer task.
    *
    * @param optimizer
    *           Cascades optimizer that scheduled this task
    * @param context
    *           search context
    * @param parent
    *           parent task
    * @param explore
    *           {@code true} if this task is for exploring, {@code false} otherwise
    * @param last
    *           {@code true} if this is the last task for a group, {@code false} otherwise
    */
   protected AbstractOptimizerTask(final CascadesQueryOptimizer optimizer, final SearchContext context,
         final OptimizerTask parent, final boolean explore, final boolean last) {
      this(optimizer, context, parent, explore, last, null);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CascadesQueryOptimizer getOptimizer() {
      return this.optimizer;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public SearchContext getSearchContext() {
      return this.context;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public OptimizerTask getParent() {
      return this.parent;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isExploring() {
      return this.explore;
   }

   /**
    * Sets whether this task is the last task for the group of expressions that is being optimized.
    *
    * @param last
    *           {@code true} if this is the last task for a group, {@code false} otherwise
    */
   protected void setLast(final boolean last) {
      this.last = last;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isLast() {
      return this.last;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Cost getBound() {
      return this.bound;
   }

   /**
    * Returns whether this task performs global epsilon pruning. This method is based on the assumption that
    * global epsilon pruning will be performed whenever the cost bound is set for a task, i.e., is not equal
    * to {@code null}.
    *
    * @return {@code true} if this task performs global epsilon pruning, {@code false} otherwise
    */
   protected boolean isGlolbalEpsilonPruning() {
      return this.bound != null;
   }
}

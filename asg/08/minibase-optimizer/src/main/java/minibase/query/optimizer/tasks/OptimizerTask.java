/*
 * @(#)OptimizerTask.java   1.0   May 24, 2014
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
import minibase.query.optimizer.SearchSpace;

/**
 * The optimization process is organized as a series of tasks, which each perform a specific activity on the
 * search space. Query optimization begins with the original task to optimize the whole query. Each task
 * creates child tasks that schedule themselves. All pending tasks are managed in a priority queue. When no
 * pending task remains, i.e., when the priority queue is empty, query optimization has finished.
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
public interface OptimizerTask {

   /**
    * Returns the Cascades optimizer that scheduled this task.
    *
    * @return optimizer
    */
   CascadesQueryOptimizer getOptimizer();

   /**
    * Returns the search context used by this task.
    *
    * @return search context
    */
   SearchContext getSearchContext();

   /**
    * Returns the parent task, i.e., the task which created this task.
    *
    * @return parent task
    */
   OptimizerTask getParent();

   /**
    * Performs the activity represented by this task.
    *
    * @param searchSpace the search space in which the task should be performed
    */
   void perform(SearchSpace searchSpace);

   /**
    * Returns whether this task is for exploring, i.e., not optimizing, only.
    *
    * @return {@code true} if this task is for exploring, {@code false} otherwise
    */
   boolean isExploring();

   /**
    * Returns whether this task is the last task for a group.
    *
    * @return {@code true} if this is the last task for a group, {@code false} otherwise
    */
   boolean isLast();

   /**
    * Returns the epsilon cost bound for global epsilon pruning.
    *
    * @return cost bound for global epsilon pruning
    */
   Cost getBound();
}

/*
 * @(#)CascadesQueryOptimizer.java   1.0   Aug 22, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.Stack;

import minibase.query.optimizer.rules.RuleManager;
import minibase.query.optimizer.tasks.OptimizeGroupTask;
import minibase.query.optimizer.tasks.OptimizerTask;

/**
 * Query optimizer implementation that follows the top-down transformation approach of the Cascades framework
 * for query optimization.
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
public class CascadesQueryOptimizer implements QueryOptimizer {

   /** Stack of optimizer tasks. */
   private final Stack<OptimizerTask> tasks;

   /** Rule manager. */
   private final RuleManager rules;

   /** Optimizer settings. */
   private final CascadesSettings settings;

   /** Optimizer statistics. */
   private final CascadesStatistics statistics;

   /**
    * Creates a new Cascades query optimizer.
    */
   public CascadesQueryOptimizer() {
      this.tasks = new Stack<>();
      this.rules = new RuleManager();
      this.settings = new CascadesSettings();
      this.statistics = new CascadesStatistics();
   }

   /**
    * Adds an optimizer task to the optimizer.
    *
    * @param task
    *           optimizer task
    */
   public void addOptimizerTasks(final OptimizerTask task) {
      this.tasks.push(task);
   }

   /**
    * Returns the rule manager used by this Cascades optimizer.
    *
    * @return rule manager
    */
   public RuleManager getRuleManager() {
      return this.rules;
   }

   /**
    * Returns the settings used by this Cascades optimizer.
    *
    * @return optimizer settings
    */
   public CascadesSettings getSettings() {
      return this.settings;
   }

   /**
    * Returns the statistics collected by this Cascades optimizer.
    *
    * @return optimizer statistics
    */
   public CascadesStatistics getStatistics() {
      return this.statistics;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Expression optimize(final Expression query) {
      final SearchSpace space = new SearchSpace(this);
      final MultiExpression expression = space.insert(query);
      return this.optimize(space, expression.getGroup(), false);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public ExplainedExpression explain(final Expression query) {
      final SearchSpace space = new SearchSpace(this);
      return this.explain(space, query);
   }

   /**
    * Optimizes the given query using an existing search space and returns an optimized query expression that
    * contains optimizer metadata. The existing search space should be empty, otherwise the result of the
    * optimization might not be valid, deterministic, or well-defined.
    *
    * @param space
    *           existing search space
    * @param query
    *           query expression
    * @return optimized query expression
    */
   public ExplainedExpression explain(final SearchSpace space, final Expression query) {
      final MultiExpression expression = space.insert(query);
      return (ExplainedExpression) this.optimize(space, expression.getGroup(), true);
   }

   /**
    * Optimizes the given root group.
    *
    * @param space
    *           search space
    * @param root
    *           root group
    * @param explain
    *           {@code true} if the returned expression should contain optimizer metadata, {@code false}
    *           otherwise
    * @return optimized query expression
    */
   private Expression optimize(final SearchSpace space, final Group root, final boolean explain) {
      // Create the initial search context with no requested physical properties and infinite upper bound.
      final PhysicalProperties requiredProperties = PhysicalProperties.anyPhysicalProperties();
      final SearchContext context = new SearchContext(requiredProperties, Cost.infinity());

      Cost epsilonBound = null;
      if (this.settings.isGlobalEpsilonPruning()) {
         epsilonBound = this.settings.getGlobalEpsilonBound();
      }
      // Start optimization from the given group and with a null parent task.
      this.addOptimizerTasks(new OptimizeGroupTask(this, context, null, true, epsilonBound, root));

      // Optimizer loop
      while (!this.tasks.empty()) {
         final OptimizerTask task = this.tasks.pop();
         // System.out.println("Performing " + task.getClass().getSimpleName() + "[exploring=" +
         // task.isExploring() + ", last=" + task.isLast() + "]");
         task.perform(space);
      }

      // Return the optimized query expression
      return space.extract(root, requiredProperties, explain);
   }
}

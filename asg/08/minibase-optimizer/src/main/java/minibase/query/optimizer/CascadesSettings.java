/*
 * @(#)CascadesSettings.java   1.0   May 29, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

/**
 * Configuration settings of the Cascades optimizer.
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
public final class CascadesSettings {

   /** Enables pruning (default: {@code false}). */
   private final boolean pruning;

   /** Enables column unique cardinality pruning (default: {@code false}). */
   private final boolean columnUniqueCardinalityPruning;

   /** Enables global epsilon pruning (default: {@code false}). */
   private final boolean globalEpsilonPruning;

   /** Cost bound for global epsilon pruning (default: {@code null}). */
   private final Cost globalEpsilonBound;

   /** Epsilon for global epsilon pruning (default: {@code 0.5}). */
   private final double epsilon;

   /** Enables inclusion of physical multi-expressions in groups (default: {@code false}). */
   private final boolean noPhysicalExpressionsInGroups;

   /** Enables cost estimation for global epsilon pruning, rather than optimization (default: {@code false}). */
   private final boolean estimateGlobalEpsilonCost;

   /** Enables halting under certain conditions before completely optimizing a query (default: {@code false}). */
   private final boolean halting;

   /** Halt when the number of plans is equal to this percentage of the group (default: {@code 100}). */
   private final int haltGroupSize;

   /** Halt when improvement is below this window size (default: {@code 3}). */
   private final int haltWindowSize;

   /** Halt when improvement is less than this percentage (default: {@code 20}). */
   private final int haltImprovement;

   /** Enables cross-join (default: {@code false}). */
   private final boolean allowingCrossJoin;

   /**
    * Constructor that creates the default configuration.
    */
   protected CascadesSettings() {
      this.pruning = false;
      this.columnUniqueCardinalityPruning = false;
      this.globalEpsilonPruning = false;
      this.globalEpsilonBound = null;
      this.epsilon = 0.5;
      this.noPhysicalExpressionsInGroups = false;
      this.estimateGlobalEpsilonCost = false;
      this.halting = false;
      this.haltGroupSize = 100;
      this.haltWindowSize = 3;
      this.haltImprovement = 20;
      this.allowingCrossJoin = false;
   }

   /**
    * Returns whether pruning is enabled.
    *
    * @return {@code true} if pruning is enable, {@code false} otherwise
    */
   public boolean isPruning() {
      return this.pruning;
   }

   /**
    * Returns whether column unique cardinality pruning is enabled.
    *
    * @return {@code true} if column unique cardinality pruning is enable, {@code false} otherwise
    */
   public boolean isColumnUniqueCardinalityPruning() {
      return this.columnUniqueCardinalityPruning;
   }

   /**
    * Returns whether global epsilon pruning is enabled.
    *
    * @return {@code true} if global epsilon pruning is enable, {@code false} otherwise
    */
   public boolean isGlobalEpsilonPruning() {
      return this.globalEpsilonPruning;
   }

   /**
    * Returns whether physical multi-expressions are included in groups or not.
    *
    * @return {@code true} if physical multi-expressions are included in groups, {@code false} otherwise
    */
   public boolean isNoPhysicalExpressionsInGroups() {
      return this.noPhysicalExpressionsInGroups;
   }

   /**
    * Returns whether the optimizer is used to obtain a cost estimate for global epsilon pruning.
    *
    * @return {@code true} if the optimizer is used to obtain a cost estimate for global epsilon pruning,
    *         {@code false} otherwise
    */
   public boolean isEstimateGlobalEpsilonCost() {
      return this.estimateGlobalEpsilonCost;
   }

   /**
    * Returns the global epsilon cost bound that the optimizer uses for pruning.
    *
    * @return global epsilon cost bound
    */
   public Cost getGlobalEpsilonBound() {
      return this.globalEpsilonBound;
   }

   /**
    * Returns the epsilon that is used for global epsilon pruning.
    *
    * @return epsilon value
    */
   public double getEpsilon() {
      return this.epsilon;
   }

   /**
    * Returns whether the optimizer is configured to halt on certain conditions.
    *
    * @return {@code true} if the optimizer halts under certain conditions, {@code false} otherwise
    */
   public boolean isHalting() {
      return this.halting;
   }

   /**
    * Returns the percentage of (logical) plans of a group that are optimized before the optimizer stops.
    *
    * @return percentage of plans
    */
   public int getHaltGroupSize() {
      return this.haltGroupSize;
   }

   /**
    * Returns the size of the window that is used to assess the improvement in optimized plans.
    *
    * @return window size
    */
   public int getHaltWindowSize() {
      return this.haltWindowSize;
   }

   /**
    * Returns the improvement threshold as a percentage under which the optimizer halts.
    *
    * @return percentage of improvement
    */
   public int getHaltImprovement() {
      return this.haltImprovement;
   }

   /**
    * Returns whether the optimizer is configured to allow cross-joins.
    *
    * @return {@code true} if the optimizer is allowed to use cross-joins, {@code false} otherwise
    */
   public boolean isAllowingCrossJoin() {
      return this.allowingCrossJoin;
   }
}

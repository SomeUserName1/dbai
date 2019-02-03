/*
 * @(#)Winner.java   1.0   May 30, 2014
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
 * A winner identifies the multi-expression that matches a given search context best. Each expression group
 * maintains a set of winners from previous searches. Winners are also used to indicate various states of
 * query optimization. A winner is marked as ready to indicate that the optimization of its plan and all its
 * inputs has completed. In contrast, a winner that is marked as not ready is still being optimized for the
 * corresponding search context. This is used by the query optimizer to detect the attempt to optimize
 * recursive queries, which is not supported. A winner that is marked as ready but has a plan than is
 * {@code null} is used to indicate that the corresponding search context cannot be satisfied by the group.
 * These {@code null} plan winners are not pruned from the group to avoid repeated attempts to optimize for a
 * search context that cannot be satisfied. For winners with a {@code null} plan, the cost represents the
 * upper bound that needs to be met during query optimization.
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
public class Winner {

   /** Winning query plan. */
   private final MultiExpression plan;

   /** Physical properties of the most recent search context that generated this winner. */
   private final PhysicalProperties physicalProperties;

   /** Cost of the most recent search context that generated this winner. */
   private final Cost cost;

   /** Indicates whether the winning query plan ready or still under construction. */
   private boolean ready;

   /**
    * Creates a new winner for the given plan that was found based on the given physical properties and cost.
    *
    * @param plan
    *           winning query plan
    * @param physicalProperties
    *           physical properties from search context
    * @param cost
    *           cost from search context
    * @param ready
    *           indicates whether the winning query plan is ready or under construction
    */
   public Winner(final MultiExpression plan, final PhysicalProperties physicalProperties, final Cost cost,
         final boolean ready) {
      this.plan = plan;
      this.physicalProperties = physicalProperties;
      this.cost = cost;
      this.ready = ready;
   }

   /**
    * Returns the winning query plan.
    *
    * @return winning query plan
    */
   public MultiExpression getPlan() {
      return this.plan;
   }

   /**
    * Returns the physical properties of the most recent search context that generated this winner.
    *
    * @return physical properties from search context
    */
   public PhysicalProperties getPhysicalProperties() {
      return this.physicalProperties;
   }

   /**
    * Returns the cost of the most recent search context that generated this winner.
    *
    * @return cost from search context
    */
   public Cost getCost() {
      return this.cost;
   }

   /**
    * Returns whether the winning query plan ready or still under construction.
    *
    * @return {@code true} if the winner is ready, {@code false} otherwise.
    */
   public boolean isReady() {
      return this.ready;
   }

   /**
    * Sets whether the winning query plan ready or still under construction.
    *
    * @param ready
    *           indicates whether the winning query plan is ready or under construction
    */
   public void setReady(final boolean ready) {
      this.ready = ready;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getClass().getSimpleName());
      result.append("[plan=");
      result.append(this.getPlan());
      result.append(", cost=");
      result.append(this.getCost());
      result.append(", order=");
      result.append(this.physicalProperties.getOrder());
      result.append(", key=");
      result.append(this.physicalProperties.getKey());
      result.append(", ready=");
      result.append(this.isReady());
      result.append("]");
      return result.toString();
   }
}

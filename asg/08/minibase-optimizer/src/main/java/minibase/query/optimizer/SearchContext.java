/*
 * @(#)SearchContext.java   1.0   May 29, 2014
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
 * The optimizer's search context consists of required physical properties and an upper bound.
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
public class SearchContext {

   /** Required physical properties. */
   private final PhysicalProperties requiredProperties;

   /** Upper cost bound. */
   private Cost upperBound;

   /** Indicates whether this context is done, i.e., whether the search is completed. */
   private boolean finished;

   /**
    * Creates a new optimization context with the given required physical properties and the given upper cost
    * bound.
    *
    * @param requiredProperties
    *           required physical properties
    * @param upperBound
    *           upper cost bound
    */
   public SearchContext(final PhysicalProperties requiredProperties, final Cost upperBound) {
      this.upperBound = upperBound;
      this.finished = false;
      if (requiredProperties != null && requiredProperties.getKey() != null
            && requiredProperties.getKey().size() > 1) {
         this.requiredProperties = requiredProperties;
         // TODO The next line of code does not seem correct.
         // this.requiredProperties = new PhysicalProperties(requiredProperties.getOrder(),
         // requiredProperties.getBestKey());
      } else {
         this.requiredProperties = requiredProperties;
      }
   }

   /**
    * Returns the required physical properties of this optimization context.
    *
    * @return required physical properties
    */
   public PhysicalProperties getRequiredProperties() {
      return this.requiredProperties;
   }

   /**
    * Returns the upper cost bound of this optimization context.
    *
    * @return upper cost bound
    */
   public Cost getUpperBound() {
      return this.upperBound;
   }

   /**
    * Sets a new upper cost bound for this optimization context.
    *
    * @param upperBound
    *           upper cost bound
    */
   public void setUpperBound(final Cost upperBound) {
      this.upperBound = upperBound;
   }

   /**
    * Returns whether this optimization context is finished, i.e., whether the search has completed.
    *
    * @return {@code true} if the search has completed, {@code false} otherwise
    */
   public boolean isFinished() {
      return this.finished;
   }

   /**
    * Sets whether this optimization context is finished, i.e., whether the search has completed.
    *
    * @param finished
    *           {@code true} if the search has completed, {@code false} otherwise
    */
   public void setFinished(final boolean finished) {
      this.finished = finished;
   }
}

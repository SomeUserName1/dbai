/*
 * @(#)OptimizerError.java   1.0   May 9, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.Optional;

/**
 * Indicates that a (runtime) error has happened during query optimization from which the query optimizer
 * could not recover.
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
public class OptimizerError extends Error {

   /** Serial version UID of this serializable class. */
   private static final long serialVersionUID = 718617956956243958L;

   /** Search space in which this error occurred. */
   private final SearchSpace space;

   /**
    * Constructs a new optimizer error with the given message.
    *
    * @param message
    *           description of this error
    */
   public OptimizerError(final String message) {
      super(message);
      this.space = null;
   }

   /**
    * Constructs a new optimizer error with the given message.
    *
    * @param message
    *           description of this error
    * @param space
    *           search space in which this error occurred
    */
   public OptimizerError(final String message, final SearchSpace space) {
      super(message);
      this.space = space;
   }

   /**
    * Constructs a new optimizer error with the given message and the given cause.
    *
    * @param message
    *           description of this error
    * @param cause
    *           cause of this error
    */
   public OptimizerError(final String message, final Throwable cause) {
      super(message, cause);
      this.space = null;
   }

   /**
    * Returns the search space in which this error occurred.
    *
    * @return search space
    */
   public Optional<SearchSpace> getSearchSpace() {
      return Optional.ofNullable(this.space);
   }
}

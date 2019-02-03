/*
 * @(#)AbstractPhysicalOperator.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.AbstractOperator;
import minibase.query.optimizer.operators.OperatorType;

/**
 * Abstract base class of all physical operators supported by the Minibase query optimizer.
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
public abstract class AbstractPhysicalOperator extends AbstractOperator implements PhysicalOperator {

   /**
    * Constructs a new physical operator of the given type.
    *
    * @param type
    *           type of the operator
    */
   protected AbstractPhysicalOperator(final OperatorType type) {
      super(type);
   }

   @Override
   public PhysicalProperties getPhysicalProperties(final PhysicalProperties... inputProperties) {
      throw new OptimizerError("Method PhysicalOperator#getPhysicalProperties() is not supported by "
            + this.getClass().getSimpleName() + ".");
   }

   /**
    * Checks that the given input number refers to a valid input of this physical operator and, if not, throws
    * an exception.
    *
    * @param inputNo
    *           input number
    */
   protected void assertInputNo(final int inputNo) {
      if (inputNo >= this.getArity()) {
         throw new OptimizerError(
               "Invalid input number for operator " + this.toString() + ": " + inputNo + ".");
      }
   }
}

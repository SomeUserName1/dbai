/*
 * @(#)AbstractElementOperator.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.element;

import minibase.query.optimizer.Cost;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.AbstractOperator;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.Schema;

/**
 * Abstract base class of all element operators supported by the Minibase query optimizer.
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
public abstract class AbstractElementOperator extends AbstractOperator implements ElementOperator {

   /**
    * Constructs a new item operator of the given type.
    *
    * @param type
    *           type of the operator
    */
   protected AbstractElementOperator(final OperatorType type) {
      super(type);
   }

   @Override
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      return LogicalElementProperties.defaultProperties();
   }

   @Override
   public Schema getSchema(final Schema... inputSchema) {
      throw new UnsupportedOperationException(
            "Method LogicalOperator#getSchema(Schema...)" + " is not supported by ElementOperator.");
   }

   @Override
   public PhysicalProperties getPhysicalProperties(final PhysicalProperties... inputProperties) {
      throw new UnsupportedOperationException(
            "Method PhyiscalOperator#getPhysicalProperties(PhysicalProperties[])"
                  + " is not supported by ElementOperator.");
   }

   @Override
   public Cost getLocalCost(final LogicalProperties localProperties,
         final LogicalProperties... inputProperties) {
      return Cost.zero();
   }

   @Override
   public boolean satisfyRequiredProperties(final PhysicalProperties requiredProperties,
         final LogicalProperties inputLogicalProperties, final int inputNo,
         final StrongReference<PhysicalProperties> inputRequiredProperties) {
      inputRequiredProperties.set(PhysicalProperties.anyPhysicalProperties());
      return true;
   }
}

/*
 * @(#)PhysicalOperator.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import minibase.query.optimizer.Cost;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.util.StrongReference;

/**
 * Interface of all physical operators supported by the Minibase query optimizer.
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
public interface PhysicalOperator extends Operator {

   /**
    * Derives the physical properties of this operator based on the given physical properties of its inputs.
    *
    * @param inputProperties
    *           physical properties of the inputs of this operator
    * @return derived physical properties of this operator
    */
   PhysicalProperties getPhysicalProperties(PhysicalProperties... inputProperties);

   /**
    * Computes the (local) cost of this physical operator based on the given local logical properties of its
    * corresponding logical operator and the given logical properties of the inputs of this logical operator.
    * The computed cost includes output but not input cost, since input costs are retrieved from the input
    * operators.
    *
    * @param localProperties
    *           local logical properties of this operator
    * @param inputProperties
    *           logical properties of the inputs of this operator
    * @return cost of this operator
    */
   Cost getLocalCost(LogicalProperties localProperties, LogicalProperties... inputProperties);

   /**
    * Checks whether the given required properties can be satisfied by this operator. The method returns
    * {@code true} if the required properties are not inherently violated by this operator and {@code false},
    * otherwise. Additionally, the method returns (a strong reference to) the properties this operator
    * requires from the given input. A reference to any physical properties, i.e., any data order and no
    * projection list, indicates that the operator itself is capable of satisfying all required properties.
    *
    * @param requiredProperties
    *           required physical properties
    * @param inputLogicalProperties
    *           logical properties of the current input
    * @param inputNo
    *           number of the current input
    * @param inputRequiredProperties
    *           reference to physical properties that this operator requires from the given input in order to
    *           satisfy the given required properties
    * @return {@code true} if the required properties can be satisfied, {@code false} otherwise
    */
   boolean satisfyRequiredProperties(PhysicalProperties requiredProperties,
         LogicalProperties inputLogicalProperties, int inputNo,
         StrongReference<PhysicalProperties> inputRequiredProperties);
}

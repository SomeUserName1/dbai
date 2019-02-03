/*
 * @(#)LogicalOperator.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.logical;

import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.Operator;
import minibase.query.schema.Schema;

/**
 * Interface of all logical operators supported by the Minibase query optimizer.
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
public interface LogicalOperator extends Operator {

   /**
    * Derives the logical properties of this operator based on the given logical properties of its inputs.
    *
    * @param inputProperties
    *           logical properties of the inputs of this operator
    * @return derived logical properties of this operator
    */
   LogicalProperties getLogicalProperties(LogicalProperties... inputProperties);

   /**
    * Derives the schema of this operator based on the given input schemas.
    * 
    * @param inputSchemas
    *           input schema
    * @return derived operator schema
    */
   Schema getSchema(Schema... inputSchemas);
}

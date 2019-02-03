/*
 * @(#)Operator.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators;

/**
 * Common interface of all operators supported by the Minibase query optimizer.
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
public interface Operator {

   /**
    * Returns the type of this operator.
    *
    * @return type of this operator
    */
   OperatorType getType();

   /**
    * Returns the human-readable name of this operator.
    *
    * @return name of this operator
    */
   String getName();

   /**
    * Returns the arity of this operator.
    *
    * @return arity of this operator
    */
   int getArity();

   /**
    * Returns {@code true} if this operator is a logical operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a logical operator, {@code false} otherwise
    */
   boolean isLogical();

   /**
    * Returns {@code true} if this operator is a physical operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a physical operator, {@code false} otherwise
    */
   boolean isPhysical();

   /**
    * Returns {@code true} if this operator is a leaf operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a leaf operator, {@code false} otherwise
    */
   boolean isLeaf();

   /**
    * Returns {@code true} if this operator is an element operator and {@code false} otherwise.
    *
    * @return {@code true} if this is an element operator, {@code false} otherwise
    */
   boolean isElement();

   /**
    * Returns {@code true} if this operator is a constant operator and {@code false} otherwise.
    *
    * @return {@code true} if this operator is a constant operator and {@code false} otherwise
    */
   boolean isConstant();

   /**
    * Returns whether or not this operator commutes, i.e., if the order of its inputs can be ignore.
    *
    * @return {@code true} if the operator commutes, {@code false} otherwise
    */
   boolean isCommuting();

   /**
    * Returns a hash code value for the object, see {@link Object#equals(Object)}. Optionally, the hash code
    * value can be computed without taking the order of the operator's input into account.
    *
    * @param ignoreInputOrder
    *           indicates if the hash code value take the order of the operator's inputs into account
    * @return hash code value for this object
    */
   int hashCode(boolean ignoreInputOrder);

   /**
    * Checks whether the given object is equal to this one, see {@link Object#equals(Object)}. Optionally, the
    * comparison can be compared without taking the order of the operator's input into account.
    *
    * @param other
    *           reference to object with which to compare
    * @param ignoreInputOrder
    *           indicates if the comparison will take the order of the operator's inputs into account
    * @return {@code true} if the two objects are equal, {@code false} otherwise
    */
   boolean equals(Object other, boolean ignoreInputOrder);
}

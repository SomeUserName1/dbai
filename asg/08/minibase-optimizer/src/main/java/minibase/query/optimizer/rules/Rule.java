/*
 * @(#)Rule.java   1.0   May 9, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.Operator;

/**
 * Rules are used by the Minibase optimizer to transform one expression into another. They consist of an
 * original pattern that matches an expression and a substitute pattern that replaces the original pattern
 * during rule application by the rule engine.
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
public interface Rule {

   /**
    * Returns the name of this rule.
    *
    * @return rule name
    */
   String getName();

   /**
    * Returns the arity of this rule, i.e., the number of leaf operators in the original pattern.
    *
    * @return rule arity
    */
   int getArity();

   /**
    * Returns the index of this rule in the rule set.
    *
    * @return rule index
    */
   int getIndex();

   /**
    * Returns the original pattern that this rule matches.
    *
    * @return original pattern.
    */
   Expression getOriginal();

   /**
    * Returns the substitute pattern used in this rule.
    *
    * @return substitute pattern
    */
   Expression getSubstitute();

   /**
    * Returns the next expression instantiation of the substitute pattern, based on last expression
    * instantiation and considering the given required physical properties.
    *
    * @param before
    *           expression before rule application
    * @param requiredProperties
    *           required physical properties
    * @return next expression instantiation
    */
   Expression nextSubstitute(Expression before, PhysicalProperties requiredProperties);

   /**
    * Returns the promise that results from applying this rule.
    *
    * @param opertator
    *           operator
    * @param context
    *           optimization context
    * @return promise value
    */
   Promise getPromise(Operator opertator, SearchContext context);

   /**
    * Checks whether this rule is applicable to a given expression.
    *
    * @param before
    *           expression before rule application
    * @param expression
    *           multi-expression that is bound to before expression
    * @param context
    *           optimizer search context
    * @return {@code true} if the rule is applicable, {@code false} otherwise
    */
   boolean isApplicable(Expression before, MultiExpression expression, SearchContext context);

   /**
    * Checks whether this rule is consistent, i.e., if the original and substitute patterns are legal. The
    * original and substitute patterns are legal if the following conditions are met.
    * <ul>
    * <li>leaves are numbered from 0, 1, 2, ..., {@link Rule#getArity()} - 1</li>
    * <li>all leaf numbers up to {@link Rule#getArity()} - 1 are used in the original pattern</li>
    * <li>each leaf number is used exactly once in the original pattern</li>
    * <li>the substitute pattern only uses numbers that occur in the original pattern (a leaf number may occur
    * 0, 1, or more times in the substitute pattern)</li>
    * <li>all operators in the original pattern are logical operators</li>
    * <li>all operators except the root operator in the physical pattern are logical operators</li>
    * </ul>
    *
    * @return {@code true} if this rule is consistent, {@code false} otherwise
    */
   boolean isConsistent();

   /**
    * Checks whether the root operator of the original pattern of this rule matches the root operator of the
    * given multi-expression.
    *
    * @param expression
    *           multi-expression
    * @return {@code true} if the two root operators match, {@code false} otherwise
    */
   boolean isRootMatch(MultiExpression expression);

   /**
    * Returns whether this rule transforms a logical expression into another logical expression.
    *
    * @return {@code true} if the substitute is a logical expression, {@code false} otherwise
    */
   boolean isLogicalToLogical();

   /**
    * Returns whether this rule transforms a logical expression into a logical expression.
    *
    * @return {@code true} if the substitute is a physical expression, {@code false} otherwise
    */
   boolean isLogicalToPhysical();

   /**
    * Returns the bit-mask of this rule. The bit-mask is used to control, which rules have already been fired
    * on a logical expression in order to prevent multiple exploration and optimization of the same
    * expression. Apart from blocking repeated execution of the same rule, some rules may also disable other
    * rules when fired. The bit-mask is represented as an integer with the bits that correspond to the ordinal
    * of the rule in the {@link RuleType} enumeration activated.
    *
    * @return bit-mask of this rule
    */
   int getBitmask();
}

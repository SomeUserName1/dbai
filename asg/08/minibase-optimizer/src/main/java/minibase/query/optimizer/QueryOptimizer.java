/*
 * @(#)QueryOptimizer.java   1.0   Jun 6, 2014
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
 * The query optimizer performs heuristic and cost-based optimization of the given query. The input query is
 * expressed as an expression tree with {@code LogicalOperator} nodes. The optimized query is returned as an
 * expression tree that consists of {@code PhysicalOperator} nodes. Additionally, the optimizer can explain a
 * query plan by returning an expression tree that is annotated with the statistics and cost computed during
 * query optimization.
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
public interface QueryOptimizer {

   /**
    * Optimizes the given query expression and returns an optimized query plan.
    *
    * @param query
    *           query expression
    * @return optimized query expression
    */
   Expression optimize(Expression query);

   /**
    * Optimizes the given query expression and returns an optimized query plan that contains optimizer
    * metadata.
    *
    * @param query
    *           query expression
    * @return optimized query expression
    */
   ExplainedExpression explain(Expression query);
}

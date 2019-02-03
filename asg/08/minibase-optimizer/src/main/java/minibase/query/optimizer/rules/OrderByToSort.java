/*
 * @(#)OrderByToSort.java   1.0   Jul 21, 2016
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
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.logical.OrderBy;
import minibase.query.optimizer.operators.physical.Sort;

/**
 * Implementation rule that transforms a {@code OrderBy} logical operator into a {@code Sort} physical
 * operator, i.e., OrderBy[S](R) -> Sort[S](R).
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
 * @author Devarsh Karekal Umashankar &lt;devarsh.karekal-umashankar@uni-konstanz.de&gt;
 * @version 1.0
 */
public class OrderByToSort extends AbstractRule {

   /**
    * Creates a new implementation rule to transform logical order-by operators into physical sort operators.
    */
   public OrderByToSort() {
      super(RuleType.ORDER_BY_TO_SORT,
            // Original
            new Expression(new OrderBy(), new Expression(new Leaf(0))),
            // New
            new Expression(new Sort(), new Expression(new Leaf(0))));
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
      final OrderBy sort = (OrderBy) before.getOperator();
      return new Expression(new Sort(sort.getSortParams(), sort.getSortOrders()), before.getInput(0));
   }
}

/*
 * @(#)SearchSpaceTest.java   1.0   May 16, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Ignore;
import org.junit.Test;

import minibase.query.optimizer.util.PlanExplainer;

/**
 * Test cases for class {@link SearchSpace}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class SearchSpaceTest extends OptimizerBaseTest {

   /**
    * Test for method {@link SearchSpace#insert(Expression)}.
    */
   @Test
   public void testInsert() {
      final CascadesQueryOptimizer optimizer = new CascadesQueryOptimizer();
      final SearchSpace space = new SearchSpace(optimizer);
      final Expression query = this.buildBSRJoinQuery();
      assertNotNull(space.insert(query));
   }

   /**
    * Test for method {@link QueryOptimizer#optimize(Expression)}.
    */
   @Test
   public void testOptimize() {
      final QueryOptimizer optimizer = new CascadesQueryOptimizer();
      optimizer.optimize(this.buildBSRJoinQuery());
   }

   /**
    * Test for {@link PlanExplainer}.
    */
   @Test
   public void testExplain() {
      final CascadesQueryOptimizer optimizer = new CascadesQueryOptimizer();
      final SearchSpace space = new SearchSpace(optimizer);
      try {
         final ExplainedExpression expression = optimizer.explain(space, this.buildBSRJoinQuery());
         final StringBuilder buffer = new StringBuilder();
         PlanExplainer.appendExplainedExpression(buffer, "", expression);
         System.out.println(buffer.toString());
      } finally {
         System.out.println(space.toString());
      }
   }
}

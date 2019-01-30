/*
 * @(#)EquiJoinToHashJoinTest.java   1.0   Jan 30, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2017 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import minibase.catalog.DataOrder;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.OptimizerBaseTest;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.physical.HashJoin;
import minibase.query.schema.ColumnReference;


public class EquiJoinToHashJoinTest extends OptimizerBaseTest {

   @Test
   public void testRuleActivated() {
      assertNotNull("The EquiJoinToHashJoin rule must be activated in the RuleManager.",
            new RuleManager().getRule(RuleType.EQUI_TO_HASH_JOIN));
   }

   @Test
   public void testPromiseNone() {
      final EquiJoinToHashJoin ejhj = new EquiJoinToHashJoin();
      final EquiJoin crossProduct = new EquiJoin(Collections.emptyList(), Collections.emptyList());
      assertEquals("The Promise should be NONE for Cross Products.", Promise.NONE, ejhj.getPromise(crossProduct,
            new SearchContext(new PhysicalProperties(DataOrder.ANY, null), Cost.infinity())));
   }
   @Test
   public void testPromiseHash() {
      final EquiJoinToHashJoin ejhj = new EquiJoinToHashJoin();
      final EquiJoin join = (EquiJoin) this.buildSimpleSRBJoinQuery().getOperator();
      assertEquals("The Promise should be HASH for non Cross Products.",
            Promise.HASH, ejhj.getPromise(join,
                  new SearchContext(new PhysicalProperties(DataOrder.ANY, null), Cost.infinity())));
   }

   @Test
   public void testNextSubstitute() {
      final EquiJoinToHashJoin ejhj = new EquiJoinToHashJoin();
      final Expression join = this.buildSimpleSRBJoinQuery().getInput(0);
      final EquiJoin eq = (EquiJoin) join.getOperator();
      final Expression subst = ejhj.nextSubstitute(join, new PhysicalProperties(DataOrder.ANY, null));
      assertEquals("EquiJoinToHashJoin must not touch the join's inputs.",
            join.getInput(0), subst.getInput(0));
      assertEquals("EquiJoinToHashJoin must not touch the join's inputs.",
            join.getInput(1), subst.getInput(1));
      assertTrue("EquiJoinToHashJoin must produce an expression with a HashJoin as root operator.",
            subst.getOperator() instanceof HashJoin);
      final HashJoin hash = (HashJoin) subst.getOperator();
      final String message = "The substitute of EquiJoinToHashJoin must have the "
            + "same join predicate(s) as the original expression.";
      assertEquals(message, eq.getLeftColumns().size(), hash.getLeftColumns().size());
      for (final ColumnReference ref : eq.getLeftColumns()) {
         assertTrue(message, hash.getLeftColumns().contains(ref));
      }
      assertEquals(message, eq.getRightColumns().size(), hash.getRightColumns().size());
      for (final ColumnReference ref : eq.getRightColumns()) {
         assertTrue(message, hash.getRightColumns().contains(ref));
      }
   }
}

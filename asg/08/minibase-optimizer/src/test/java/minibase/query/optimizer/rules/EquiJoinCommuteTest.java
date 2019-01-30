/*
 * @(#)EquiJoinCommuteTest.java   1.0   Jan 30, 2017
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import minibase.catalog.DataOrder;
import minibase.query.QueryException;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.OptimizerBaseTest;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.schema.ColumnReference;

public class EquiJoinCommuteTest extends OptimizerBaseTest {
   @Test
   public void testRuleType() {
      final EquiJoinCommute op = new EquiJoinCommute();
      assertEquals("Wrong RuleType for EquiJoinCommute: " + op.getName(),
            RuleType.EQUI_JOIN_COMMUTE.name(), op.getName());
      assertEquals("Wrong RuleType arity for EquiJoinCommute: " + op.getArity(),
            RuleType.EQUI_JOIN_COMMUTE.getArity(), op.getArity());
   }

   @Test
   public void testInputPattern() {
      final EquiJoinCommute op = new EquiJoinCommute();
      final Expression inputPattern = op.getOriginal();
      assertEquals("Root operator of original pattern should be an EquiJoin, found "
               + inputPattern.getOperator().getName(),
            OperatorType.EQJOIN, inputPattern.getOperator().getType());
      final Expression leftInput = inputPattern.getInput(0);
      final Operator leftOp = leftInput.getOperator();
      assertEquals("The left input of the join in the original pattern must be a Leaf, found " + leftOp.getName(),
            OperatorType.LEAF, leftOp.getType());
      final Expression rightInput = inputPattern.getInput(1);
      final Operator rightOp = rightInput.getOperator();
      assertEquals("The right input of the join in the original pattern must be a Leaf, found " + rightOp.getName(),
            OperatorType.LEAF, rightOp.getType());
      assertNotEquals("The IDs of both Leaf operators of the original pattern must be different.",
            ((Leaf) leftOp).getIndex(), ((Leaf) rightOp).getIndex());
   }

   @Test
   public void testOutputPattern() {
      final EquiJoinCommute op = new EquiJoinCommute();
      final Expression outputPattern = op.getSubstitute();
      assertEquals("Root operator of substitute pattern should be an EquiJoin, found "
               + outputPattern.getOperator().getName(),
            OperatorType.EQJOIN, outputPattern.getOperator().getType());
      final Expression leftInput = outputPattern.getInput(0);
      final Operator leftOp = leftInput.getOperator();
      assertEquals("The left input of the join in the substitute pattern must be a Leaf, found " + leftOp.getName(),
            OperatorType.LEAF, leftOp.getType());
      final Expression rightInput = outputPattern.getInput(1);
      final Operator rightOp = rightInput.getOperator();
      assertEquals("The right input of the join in the substitute pattern must be a Leaf, found " + rightOp.getName(),
            OperatorType.LEAF, rightOp.getType());
      assertNotEquals("The IDs of both Leaf operators of the substitute pattern must be different.",
            ((Leaf) leftOp).getIndex(), ((Leaf) rightOp).getIndex());
   }

   @Test
   public void testPatternConsistency() {
      final EquiJoinCommute op = new EquiJoinCommute();
      final Expression inputPattern = op.getOriginal();
      final Expression leftInInput = inputPattern.getInput(0);
      final Operator leftInOp = leftInInput.getOperator();
      final Expression rightInInput = inputPattern.getInput(1);
      final Operator rightInOp = rightInInput.getOperator();
      final Expression outputPattern = op.getSubstitute();
      final Expression leftOutInput = outputPattern.getInput(0);
      final Operator leftOutOp = leftOutInput.getOperator();
      final Expression rightOutInput = outputPattern.getInput(1);
      final Operator rightOutOp = rightOutInput.getOperator();
      if (leftInOp instanceof Leaf && rightInOp instanceof Leaf
            && leftOutOp instanceof Leaf && rightOutOp instanceof Leaf) {
         assertEquals("The left Leaf in the original pattern must have the same index "
                  + "as the right one of the substitute pattern.",
               ((Leaf) leftInOp).getIndex(), ((Leaf) rightOutOp).getIndex());
         assertEquals("The right Leaf in the original pattern must have the same index "
                  + "as the left one of the substitute pattern.",
               ((Leaf) rightInOp).getIndex(), ((Leaf) leftOutOp).getIndex());
      }
   }

   @Test
   public void testBitMask() {
      final EquiJoinCommute op = new EquiJoinCommute();
      assertTrue("The EquiJoinCommute's bit-mask must have a bit set for EquiJoinCommute",
            (op.getBitmask() & (1 << RuleType.EQUI_JOIN_COMMUTE.ordinal())) != 0);
   }

   @Test
   public void testRuleManager() {
      final RuleManager manager = new RuleManager();
      assertNotNull("EquiJoinCommute must be added to the RuleManager.",
            manager.getRule(RuleType.EQUI_JOIN_COMMUTE));
   }

   @Test
   public void testNextSubstitute() throws QueryException {
      final Expression rootExpression = this.buildSimpleSRBJoinQuery();
      System.out.println(rootExpression);
      final EquiJoinCommute rule = new EquiJoinCommute();
      final Expression substitute = rule.nextSubstitute(rootExpression, new PhysicalProperties(DataOrder.ANY, null));
      assertEquals("The root operator of the substitute expression must be an EquiJoin.",
            OperatorType.EQJOIN, substitute.getOperator().getType());
      assertEquals("The first input of the original expression must be equal to "
               + "the second input of the transformed expression.",
            rootExpression.getInput(0), substitute.getInput(1));
      assertEquals("The second input of the original expression must be equal to "
               + "the first input of the transformed expression.",
            rootExpression.getInput(1), substitute.getInput(0));
      final EquiJoin before = (EquiJoin) rootExpression.getOperator();
      final EquiJoin after = (EquiJoin) substitute.getOperator();
      assertEquals("Mismatched lengths of the join columns.",
            before.getLeftColumns().size(), after.getRightColumns().size());
      assertEquals("Mismatched lengths of the join columns.",
            before.getRightColumns().size(), after.getLeftColumns().size());
      for (final ColumnReference ref : before.getLeftColumns()) {
         assertTrue("Column missing in right join columns: " + ref, after.getRightColumns().contains(ref));
      }
      for (final ColumnReference ref : before.getRightColumns()) {
         assertTrue("Column missing in left join columns: " + ref, after.getLeftColumns().contains(ref));
      }
   }
}

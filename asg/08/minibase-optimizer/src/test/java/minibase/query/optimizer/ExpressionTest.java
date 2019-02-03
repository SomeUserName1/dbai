/*
 * @(#)ExpressionTest.java   1.0   May 10, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import minibase.query.optimizer.ExpressionIteratorFactory.Traversal;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.rules.Leaf;

/**
 * Test cases for class {@link Expression}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class ExpressionTest {

   /**
    * Test case for method {@link Expression#iterator()}.
    */
   @Test
   public void testIterator() {
      final Expression left = new Expression(new Leaf(0));
      final Expression right = new Expression(new Leaf(1));
      final Expression rootLeft = new Expression(new EquiJoin(), left, right);
      final Expression rootRight = new Expression(new Leaf(2));
      final Expression root = new Expression(new EquiJoin(), rootLeft, rootRight);
      Iterator<Expression> iterator = ExpressionIteratorFactory.getIterator(root, Traversal.PREORDER);
      assertTrue(iterator.hasNext());
      assertSame(iterator.next(), root);
      assertSame(iterator.next(), rootLeft);
      assertSame(iterator.next(), left);
      assertSame(iterator.next(), right);
      assertSame(iterator.next(), rootRight);
      assertFalse(iterator.hasNext());
      iterator = ExpressionIteratorFactory.getIterator(root, Traversal.INORDER);
      assertTrue(iterator.hasNext());
      assertSame(iterator.next(), left);
      assertSame(iterator.next(), rootLeft);
      assertSame(iterator.next(), right);
      assertSame(iterator.next(), root);
      assertSame(iterator.next(), rootRight);
      assertFalse(iterator.hasNext());
      iterator = ExpressionIteratorFactory.getIterator(root, Traversal.POSTORDER);
      assertTrue(iterator.hasNext());
      assertSame(iterator.next(), left);
      assertSame(iterator.next(), right);
      assertSame(iterator.next(), rootLeft);
      assertSame(iterator.next(), rootRight);
      assertSame(iterator.next(), root);
      assertFalse(iterator.hasNext());
   }
}

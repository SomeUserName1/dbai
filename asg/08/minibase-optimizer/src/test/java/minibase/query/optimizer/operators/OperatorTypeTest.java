/*
 * @(#)OperatorTypeTest.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test cases for class {@link OperatorType}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class OperatorTypeTest {

   /**
    * Tests the <em>get</em> operator type.
    */
   @Test
   public void testGet() {
      final OperatorType type = OperatorType.GETTABLE;
      assertEquals(0, type.getArity());
      assertTrue(type.isLogical());
      assertFalse(type.isPhysical());
      assertFalse(type.isLeaf());
      assertFalse(type.isElement());
   }
}

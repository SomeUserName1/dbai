/*
 * @(#)RuleTest.java   1.0   May 10, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * General test cases for rules.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class RuleTest {

   /**
    * Test whether the currently defined rules are internally consistent.
    */
   @Test
   public void testCheck() {
      final RuleManager rules = new RuleManager();
      for (final Rule rule : rules.getActiveRules()) {
         assertTrue(rule.isConsistent());
      }
   }
}

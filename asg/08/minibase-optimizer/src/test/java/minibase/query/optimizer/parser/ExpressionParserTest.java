/*
 * @(#)ExpressionParserTest.java   1.0   Jun 28, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.parser;

import static org.junit.Assert.fail;

import java.io.InputStream;

import org.junit.Test;

import minibase.query.optimizer.OptimizerBaseTest;
import minibase.util.ResourceUtil;

/**
 * Test cases to check the parser for Cascades/Columbia-style query expressions.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class ExpressionParserTest extends OptimizerBaseTest {

   /** Query file used in the tests. */
   private static final String QUERY_FILE = "minibase/query/optimizer/Sailors.qry";

   /**
    * Test case for the query expression parser.
    */
   @Test
   public void testParser() {
      final InputStream input = ResourceUtil.getResourceAsStream(QUERY_FILE);
      try {
         final QueryExpression expression = QueryExpressionParser.parse(input);
         System.out.println(expression.toString());
      } catch (final ParseException e) {
         System.out.println("Query parsing failed, stack trace follows:" + e);
         fail();
      }
   }
}

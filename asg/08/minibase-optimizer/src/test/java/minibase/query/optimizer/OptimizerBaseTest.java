/*
 * @(#)OptimizerBaseTest.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.junit.BeforeClass;

import minibase.catalog.SystemCatalog;
import minibase.catalog.TransientSystemCatalog;
import minibase.query.optimizer.parser.QueryExpression;
import minibase.query.optimizer.parser.QueryExpressionParser;
import minibase.query.schema.ReferenceTable;
import minibase.util.ResourceUtil;
import minibase.util.XmlToSystemCatalog;

/**
 * Base class for all tests in the test suite for the Minibase optimizer. This base class creates an example
 * schema and catalog information that can be used in the optimizer tests.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class OptimizerBaseTest {

   /** System catalog used in the optimizer tests. */
   private static SystemCatalog catalog;

   /** System catalog XML file. */
   private static final String CATALOG_XML = "minibase/query/optimizer/Sailors_catalog.xml";

   /** Query file used in the tests. */
   private static final String QUERY_FILE = "minibase/query/optimizer/Sailors.qry";

   /**
    * Returns the system catalog used in the optimizer tests.
    *
    * @return system catalog
    */
   protected SystemCatalog getSystemCatalog() {
      return catalog;
   }

   /**
    * Set up method that loads a simple system catalog.
    *
    * @throws Exception
    *            if system catalog cannot be loaded
    */
   @BeforeClass
   public static void loadSystemCatalog() throws Exception {
      catalog = new TransientSystemCatalog(4096);
      XmlToSystemCatalog.loadFrom(catalog, ResourceUtil.getResourceAsStream(CATALOG_XML));
   }

   /**
    * Parses and returns a query expression that joins the "Boats" and "Reserves" tables on attribute "bid" as
    * well as the "Sailors" and "Reserves" tables on attribute "sid".
    *
    * @return query expression
    */
   protected Expression buildBSRJoinQuery() {
      try {
         final InputStream input = ResourceUtil.getResourceAsStream(QUERY_FILE);
         final QueryExpression query = QueryExpressionParser.parse(input);
         return query.toExpression(new ReferenceTable(catalog));
      } catch (final Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected Expression buildSimpleSRBJoinQuery() {
      final String query = "(EQJOIN(R.bid, B.bid),"
            + "   (EQJOIN(S.sid, R.sid),"
            + "      GET(Sailors, S),"
            + "      GET(Reserves, R)"
            + "   ),"
            + "   GET(Boats, B)"
            + ")";
      try (InputStream input = new ByteArrayInputStream(query.getBytes(Charset.forName("UTF-8")))) {
         final QueryExpression expr = QueryExpressionParser.parse(input);
         return expr.toExpression(new ReferenceTable(this.getSystemCatalog()));
      } catch (final Exception e) {
         throw new RuntimeException(e);
      }
   }
}

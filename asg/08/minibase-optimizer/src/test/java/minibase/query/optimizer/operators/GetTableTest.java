/*
 * @(#)GetTableTest.java   1.0   Feb 14, 2014
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import minibase.catalog.SailorsCatalogUtil;
import minibase.catalog.SystemCatalog;
import minibase.catalog.TableDescriptor;
import minibase.query.QueryException;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.OptimizerBaseTest;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.schema.ReferenceTable;
import minibase.query.schema.StoredTableReference;

/**
 * Test cases for class {@link GetTable}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class GetTableTest extends OptimizerBaseTest {

   /**
    * Tests the {@code hashCode()} method.
    * @throws QueryException exception
    */
   @Test
   public void testHashCode() throws QueryException {
      final SystemCatalog catalog = this.getSystemCatalog();
      final ReferenceTable refTable = new ReferenceTable(catalog);

      final StoredTableReference s1 = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S1");
      final StoredTableReference s2 = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S2");
      final StoredTableReference s3 = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S3");

      final GetTable op1 = new GetTable(s1);
      final GetTable op2 = new GetTable(s2);
      assertNotEquals(op1.hashCode(), op2.hashCode());
      final GetTable op3 = new GetTable(s3);
      assertNotEquals(op1.hashCode(), op3.hashCode());
   }

   /**
    * Tests the {@code equals()} method.
    * @throws QueryException exception
    */
   @Test
   public void testEquals() throws QueryException {
      final SystemCatalog catalog = this.getSystemCatalog();
      final ReferenceTable refTable = new ReferenceTable(catalog);

      final StoredTableReference s1 = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S1");
      final StoredTableReference s2 = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S2");

      final GetTable op1 = new GetTable(s1);
      final GetTable op2 = new GetTable(s2);
      assertFalse(op1.equals(op2));
      assertFalse(op2.equals(op1));
      final GetTable op3 = new GetTable(s1);
      assertTrue(op1.equals(op3));
      assertTrue(op3.equals(op1));
   }

   /**
    * Tests the logical properties.
    * @throws QueryException exception
    */
   @Test
   public void testLogicalProperties() throws QueryException {
      final SystemCatalog catalog = this.getSystemCatalog();
      final ReferenceTable refTable = new ReferenceTable(catalog);

      final TableDescriptor sailors = catalog.getTable(SailorsCatalogUtil.TBL_SAILORS);
      final StoredTableReference s = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S");
      final GetTable getS = new GetTable(s);
      final LogicalCollectionProperties sProps = getS.getLogicalProperties();
      assertEquals(sailors.getColumns().size(), sProps.getSchema().getColumnCount());

      final TableDescriptor boats = catalog.getTable(SailorsCatalogUtil.TBL_BOATS);
      final StoredTableReference b = refTable.insertTable(SailorsCatalogUtil.TBL_BOATS, "B");
      final GetTable getB = new GetTable(b);
      final LogicalCollectionProperties bProps = getB.getLogicalProperties();
      assertEquals(boats.getColumns().size(), bProps.getSchema().getColumnCount());

      final TableDescriptor reserves = catalog.getTable(SailorsCatalogUtil.TBL_RESERVES);
      final StoredTableReference r = refTable.insertTable(SailorsCatalogUtil.TBL_RESERVES, "R");
      final GetTable getR = new GetTable(r);
      final LogicalCollectionProperties rProps = getR.getLogicalProperties();
      assertEquals(reserves.getColumns().size(), rProps.getSchema().getColumnCount());
   }
}

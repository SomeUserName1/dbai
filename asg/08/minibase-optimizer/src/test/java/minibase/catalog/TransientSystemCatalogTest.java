/*
 * @(#)TransientSystemCatalogTest.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import org.junit.Test;

import minibase.TestHelper;
import minibase.query.optimizer.OptimizerBaseTest;
import minibase.storage.file.DiskManager;

/**
 * Test cases for class {@link TransientSystemCatalog}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class TransientSystemCatalogTest extends OptimizerBaseTest {

   /**
    * Build a simple system catalog.
    */
   @Test
   public void testSimpleCatalog() {
      final SystemCatalog catalog = this.getSystemCatalog();
      System.out.println(catalog.toSQL());
   }

   /**
    * Test that an Exception is thrown when we try to create a table or column that already exist.
    */
   @Test
   public void testCreatesThrowExceptionIfAlreadyExists() {

      final SystemCatalog catalog = this.getSystemCatalog();

      final TableStatistics tableStatistics = new TableStatistics(SailorsCatalogUtil.SAILORS_CARD, 0,
            SailorsCatalogUtil.SAILORS_WIDTH);
      TestHelper.assertThrows(IllegalStateException.class,
            () -> catalog.createTable(tableStatistics, SailorsCatalogUtil.TBL_SAILORS));

      final ColumnStatistics columnStatistics = new ColumnStatistics(SailorsCatalogUtil.SAILORS_CARD,
            SailorsCatalogUtil.SAILORS_CARD, 0, SailorsCatalogUtil.SAILORS_CARD,
            (double) 4 / DiskManager.PAGE_SIZE);
      TestHelper.assertThrows(IllegalStateException.class,
            () -> catalog.createColumn(columnStatistics, SailorsCatalogUtil.ATT_SID, DataType.INT, 0,
                  catalog.getTable(SailorsCatalogUtil.TBL_SAILORS).getCatalogID()));
   }
}

/*
 * @(#)EquiJoinTest.java   1.0   May 1, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators;

import java.util.Collections;

import org.junit.Test;

import minibase.catalog.SailorsCatalogUtil;
import minibase.catalog.SystemCatalog;
import minibase.query.QueryException;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.OptimizerBaseTest;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.ReferenceTable;
import minibase.query.schema.StoredTableReference;

/**
 * Test cases for class {@link EquiJoin}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class EquiJoinTest extends OptimizerBaseTest {

   /**
    * Test logical properties.
    * @throws QueryException exception
    */
   @Test
   public void testLogicalProperties() throws QueryException {
      final SystemCatalog catalog = this.getSystemCatalog();
      final ReferenceTable refTable = new ReferenceTable(catalog);

      final StoredTableReference sailors = refTable.insertTable(SailorsCatalogUtil.TBL_SAILORS, "S");
      final StoredTableReference reserves = refTable.insertTable(SailorsCatalogUtil.TBL_RESERVES, "R");

      final GetTable getS = new GetTable(sailors);
      final GetTable getR = new GetTable(reserves);

      final ColumnReference sSid = refTable.resolveColumn("S", SailorsCatalogUtil.ATT_SID).get();
      final ColumnReference rSid = refTable.resolveColumn("R", SailorsCatalogUtil.ATT_SID).get();
      final EquiJoin joinRS = new EquiJoin(Collections.singletonList(sSid), Collections.singletonList(rSid));
      final LogicalCollectionProperties joinProps = joinRS.getLogicalProperties(getS.getLogicalProperties(),
            getR.getLogicalProperties());
      System.out.println(joinProps.toString());
      System.out.println(joinProps.getSchema().toString());
   }
}

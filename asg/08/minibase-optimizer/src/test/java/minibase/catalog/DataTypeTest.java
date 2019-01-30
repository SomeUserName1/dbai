/*
 * @(#)DataTypeTest.java   1.0   Jun 16, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Optional;

import org.junit.Test;

/**
 * Test cases for the methods of the {@link DataType} enumeration.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class DataTypeTest {

   /**
    * Test case to check the compatibility matrix of types.
    */
   @Test
   public void testCommonType() {
      assertCommonType(DataType.BIGINT, DataType.BIGINT, DataType.BIGINT);
      assertCommonType(DataType.BIGINT, DataType.INT, DataType.BIGINT);
      assertCommonType(DataType.BIGINT, DataType.SMALLINT, DataType.BIGINT);
      assertCommonType(DataType.BIGINT, DataType.TINYINT, DataType.BIGINT);
      assertCommonType(DataType.BIGINT, DataType.DOUBLE, DataType.DOUBLE);
      assertCommonType(DataType.BIGINT, DataType.FLOAT, DataType.DOUBLE);
      assertCommonType(DataType.BIGINT, DataType.DATE, null);
      assertCommonType(DataType.BIGINT, DataType.TIME, null);
      assertCommonType(DataType.BIGINT, DataType.DATETIME, null);
      assertCommonType(DataType.BIGINT, DataType.CHAR, null);
      assertCommonType(DataType.BIGINT, DataType.VARCHAR, null);
      assertCommonType(DataType.INT, DataType.INT, DataType.INT);
      assertCommonType(DataType.INT, DataType.SMALLINT, DataType.INT);
      assertCommonType(DataType.INT, DataType.TINYINT, DataType.INT);
      assertCommonType(DataType.INT, DataType.DOUBLE, DataType.DOUBLE);
      assertCommonType(DataType.INT, DataType.FLOAT, DataType.FLOAT);
      assertCommonType(DataType.INT, DataType.DATE, null);
      assertCommonType(DataType.INT, DataType.TIME, null);
      assertCommonType(DataType.INT, DataType.DATETIME, null);
      assertCommonType(DataType.INT, DataType.CHAR, null);
      assertCommonType(DataType.INT, DataType.VARCHAR, null);
      assertCommonType(DataType.SMALLINT, DataType.SMALLINT, DataType.SMALLINT);
      assertCommonType(DataType.SMALLINT, DataType.TINYINT, DataType.SMALLINT);
      assertCommonType(DataType.SMALLINT, DataType.DOUBLE, DataType.DOUBLE);
      assertCommonType(DataType.SMALLINT, DataType.FLOAT, DataType.FLOAT);
      assertCommonType(DataType.SMALLINT, DataType.DATE, null);
      assertCommonType(DataType.SMALLINT, DataType.TIME, null);
      assertCommonType(DataType.SMALLINT, DataType.DATETIME, null);
      assertCommonType(DataType.SMALLINT, DataType.CHAR, null);
      assertCommonType(DataType.SMALLINT, DataType.VARCHAR, null);
      assertCommonType(DataType.TINYINT, DataType.TINYINT, DataType.TINYINT);
      assertCommonType(DataType.TINYINT, DataType.DOUBLE, DataType.DOUBLE);
      assertCommonType(DataType.TINYINT, DataType.FLOAT, DataType.FLOAT);
      assertCommonType(DataType.TINYINT, DataType.DATE, null);
      assertCommonType(DataType.TINYINT, DataType.TIME, null);
      assertCommonType(DataType.TINYINT, DataType.DATETIME, null);
      assertCommonType(DataType.TINYINT, DataType.CHAR, null);
      assertCommonType(DataType.TINYINT, DataType.VARCHAR, null);
      assertCommonType(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE);
      assertCommonType(DataType.DOUBLE, DataType.FLOAT, DataType.DOUBLE);
      assertCommonType(DataType.DOUBLE, DataType.DATE, null);
      assertCommonType(DataType.DOUBLE, DataType.TIME, null);
      assertCommonType(DataType.DOUBLE, DataType.DATETIME, null);
      assertCommonType(DataType.DOUBLE, DataType.CHAR, null);
      assertCommonType(DataType.DOUBLE, DataType.VARCHAR, null);
      assertCommonType(DataType.FLOAT, DataType.FLOAT, DataType.FLOAT);
      assertCommonType(DataType.FLOAT, DataType.DATE, null);
      assertCommonType(DataType.FLOAT, DataType.TIME, null);
      assertCommonType(DataType.FLOAT, DataType.DATETIME, null);
      assertCommonType(DataType.FLOAT, DataType.CHAR, null);
      assertCommonType(DataType.FLOAT, DataType.VARCHAR, null);
      assertCommonType(DataType.DATE, DataType.DATE, DataType.DATE);
      assertCommonType(DataType.DATE, DataType.TIME, null);
      assertCommonType(DataType.DATE, DataType.DATETIME, null);
      assertCommonType(DataType.DATE, DataType.CHAR, null);
      assertCommonType(DataType.DATE, DataType.VARCHAR, null);
      assertCommonType(DataType.TIME, DataType.TIME, DataType.TIME);
      assertCommonType(DataType.TIME, DataType.DATETIME, null);
      assertCommonType(DataType.TIME, DataType.CHAR, null);
      assertCommonType(DataType.TIME, DataType.VARCHAR, null);
      assertCommonType(DataType.DATETIME, DataType.DATETIME, DataType.DATETIME);
      assertCommonType(DataType.DATETIME, DataType.CHAR, null);
      assertCommonType(DataType.DATETIME, DataType.VARCHAR, null);
      assertCommonType(DataType.CHAR, DataType.CHAR, DataType.CHAR);
      assertCommonType(DataType.CHAR, DataType.VARCHAR, null);
      assertCommonType(DataType.VARCHAR, DataType.VARCHAR, DataType.VARCHAR);
   }

   /**
    * Tests whether the common data type of the left and right data type is the given data type.
    *
    * @param left
    *           left data type
    * @param right
    *           right data type
    * @param expected
    *           expected data type
    */
   private static void assertCommonType(final DataType left, final DataType right, final DataType expected) {
      final Optional<DataType> l2r = left.getCommonType(right);
      final Optional<DataType> r2l = right.getCommonType(left);
      assertEquals(l2r, r2l);
      if (l2r.isPresent() && r2l.isPresent()) {
         assertEquals(l2r.get(), expected);
         assertEquals(r2l.get(), expected);
      } else {
         assertNull(expected);
      }
   }
}

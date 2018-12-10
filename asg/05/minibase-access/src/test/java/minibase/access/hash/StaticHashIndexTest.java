/*
 * @(#)StaticHashIndexTest.java   1.0   Nov 16, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import minibase.access.index.Index;
import minibase.access.index.IndexTest;

/**
 * Test suite for the static hash index.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public class StaticHashIndexTest extends IndexTest {

   @Override
   protected Index openIndex(final String fileName, final int searchKeyLength) {
      return StaticHashIndex.openIndex(getBufferManager(), fileName);
   }

   @Override
   protected Index createIndex(final String fileName, final int searchKeyLength) {
      return StaticHashIndex.createIndex(getBufferManager(), fileName, searchKeyLength);
   }

   @Override
   protected Index createTempIndex(final int searchKeyLength) {
      return StaticHashIndex.createTemporaryIndex(getBufferManager(), searchKeyLength);
   }
}

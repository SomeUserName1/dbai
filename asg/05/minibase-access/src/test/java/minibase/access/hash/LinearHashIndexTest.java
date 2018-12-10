/*
 * @(#)LinearHashIndexTest.java   1.0   Jan 13, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.util.Objects;

import minibase.access.index.Index;
import minibase.access.index.IndexTest;

/**
 * Test suite for the linear hash index.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
 * @version 1.0
 */
public class LinearHashIndexTest extends IndexTest {

   @Override
   protected Index openIndex(final String fileName, final int searchKeyLength) {
      return LinearHashIndex.openIndex(this.getBufferManager(), fileName, searchKeyLength);
   }

   @Override
   protected Index createIndex(final String fileName, final int searchKeyLength) {
      return LinearHashIndex.createIndex(this.getBufferManager(), Objects.requireNonNull(fileName),
            searchKeyLength);
   }

   @Override
   protected Index createTempIndex(final int searchKeyLength) {
      return LinearHashIndex.createTemporaryIndex(this.getBufferManager(), searchKeyLength);
   }
}

/*
 * @(#)HashIndexType.java   1.0   Oct 28, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import minibase.storage.buffer.PageID;

/**
 * Defines the different hash index types in minibase.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
 * @author Manfred Schaefer &lt;manfred.schaefer@uni-konstanz.de&gt
 * @version 1.0
 */
enum HashIndexType {
   /** Linear hash index. */
   LHASH,
   /** Extendible hash index. */
   EHASH(Byte.BYTES),
   /** Static hash index. */
   SHASH;

   /** Size of additional data. */
   private int moreDataSize;

   /**
    * Constructor.
    *
    * @param moreDataSize
    *           number of bytes for a directory entry
    */
   HashIndexType(final int moreDataSize) {
      this.moreDataSize = moreDataSize;
   }

   /**
    * Constructor.
    */
   HashIndexType() {
      this.moreDataSize = 0;
   }

   /**
    * @return number of bytes a directory entry needs
    */
   int directoryEntrySize() {
      return PageID.SIZE + this.moreDataSize;
   }

   /**
    * @return {@code true} if the index uses a depth, {@code false} otherwise
    */
   boolean hasDepth() {
      return this.moreDataSize > 0;
   }
}

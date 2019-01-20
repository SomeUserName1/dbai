/*
 * @(#)BTreePage.java   1.0   Oct 23, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.btree;

import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageType;
import minibase.storage.file.DiskManager;

/**
 * Utility methods for reading and writing a BTree page.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
abstract class BTreePage implements PageType {
   /** The position of the meta data. */
   static final int META_POS = DiskManager.PAGE_SIZE - 4;

   /** Hidden default constructor. */
   BTreePage() {
      throw new AssertionError();
   }

   /**
    * Checks if this page is a leaf page.
    * @param page the page
    * @return {@code true} if this is a leaf page, {@code false} otherwise
    */
   static boolean isLeafPage(final Page<? extends BTreePage> page) {
      return page.readInt(META_POS) < 0;
   }

   /**
    * Gets the number of keys stored in this page.
    * If this page is a branch page, this is one less than the number of child references.
    * @param page the page
    * @return number of keys
    */
   static int getNumKeys(final Page<? extends BTreePage> page) {
      return page.readInt(META_POS) & 0x7FFFFFFF;
   }

   /**
    * Sets the number of keys stored in this page.
    * @param page the page
    * @param keys new number of keys
    */
   static void setNumKeys(final Page<? extends BTreePage> page, final int keys) {
      setMeta(page, isLeafPage(page), keys);
   }

   /**
    * Sets the meta information stored in this page.
    * @param page the page
    * @param leaf leaf flag
    * @param keys number of keys
    */
   static void setMeta(final Page<? extends BTreePage> page, final boolean leaf, final int keys) {
      page.writeInt(META_POS, leaf ? 0x80000000 | keys : 0x7FFFFFFF & keys);
   }
}

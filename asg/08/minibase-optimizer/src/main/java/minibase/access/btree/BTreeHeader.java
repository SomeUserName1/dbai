/*
 * @(#)BTreeHeader.java   1.0   Oct 23, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.btree;

import minibase.SearchKeyType;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.PageType;

/**
 * The header page of a {@link BTreeIndex}. It contains the root page ID and the size of the index.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
final class BTreeHeader implements PageType {
   /** Offset to the root page ID. */
   private static final int ROOT_ID_OFFSET = 0;
   /** Offset to the number of entries in the index. */
   private static final int SIZE_OFFSET = 4;
   /** Offset to the type of the keys. */
   private static final int KEY_TYPE_OFFSET = 8;

   /** Hidden default constructor. */
   private BTreeHeader() {
      throw new AssertionError();
   }

   /**
    * Initializes a newly allocated page as a B+ tree header page.
    * @param bufferManager the buffer manager for getting the new page
    * @param rootID the root page ID
    * @return the initialized header page
    */
   static Page<BTreeHeader> newPage(final BufferManager bufferManager, final PageID rootID) {
      @SuppressWarnings("unchecked")
      final Page<BTreeHeader> header = (Page<BTreeHeader>) bufferManager.newPage();
      setRootID(header, rootID);
      setSize(header, 0);
      return header;
   }

   /**
    * Gets the current root ID of the B+-tree.
    * @param page the header page
    * @return the current root page ID
    */
   static PageID getRootID(final Page<BTreeHeader> page) {
      return page.readPageID(ROOT_ID_OFFSET);
   }

   /**
    * Sets the current root ID of the B+-tree.
    * @param page the header page
    * @param rootID the new root page ID
    */
   static void setRootID(final Page<BTreeHeader> page, final PageID rootID) {
      page.writePageID(ROOT_ID_OFFSET, rootID);
   }

   /**
    * Gets the current size of the index.
    * @param page the header page
    * @return the size of the index
    */
   static int getSize(final Page<BTreeHeader> page) {
      return page.readInt(SIZE_OFFSET);
   }

   /**
    * Sets the current size of the index.
    * @param page the header page
    * @param size the new size of the index
    */
   static void setSize(final Page<BTreeHeader> page, final int size) {
      page.writeInt(SIZE_OFFSET, size);
   }

   /**
    * Gets the current type of the index.
    * @param page the header page
    * @return the size of the index
    */
   static SearchKeyType getKeyType(final Page<BTreeHeader> page) {
      final byte[] data = page.getData();
      return SearchKeyType.readFrom(data, KEY_TYPE_OFFSET);
   }

   /**
    * Sets the key type of the index.
    * @param page the header page
    * @param type the type id
    */
   public static void setKeyType(final Page<BTreeHeader> page, final SearchKeyType type) {
      type.writeTo(page.getData(), KEY_TYPE_OFFSET);
   }
}

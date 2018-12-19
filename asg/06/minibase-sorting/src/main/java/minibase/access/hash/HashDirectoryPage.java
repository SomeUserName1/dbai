/*
 * @(#)HashDirectoryPage.java   1.0   Sep 17, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.util.Arrays;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.PageType;
import minibase.storage.file.DiskManager;

/**
 * The directory page of a linear hash index.
 *
 * <pre>
 * ┌─────────────────┐
 * │HashDirectoryPage│
 * ├─────────────────┴───────────────────────────────────────────────────────────────────────────┬─────────┬──────────┐
 * │                                                                                             │WRITABLE_│NEXT_PAGE_│
 * │                                           data[]                                            │  SIZE   │ POINTER  │
 * │                                                                                             │         │          │
 * └─────────────────────────────────────────────────────────────────────────────────────────────┴────▲────┴────▲─────┘
 *                                                                                                    │         │
 *                                                                                                    │    PageID.SIZE
 *                                                                                               Short.BYTES
 * </pre>
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni.kn&gt;
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
class HashDirectoryPage implements PageType {

   /** Offset of pointer to the next directory page. */
   protected static final int NEXT_PAGE_POINTER_OFFSET = DiskManager.PAGE_SIZE - PageID.BYTES;

   /** Offset of writable size field. */
   protected static final int WRITABLE_SIZE_OFFSET = NEXT_PAGE_POINTER_OFFSET - Short.BYTES;

   /** Offset of directory. */
   protected static final int DIRECTORY_OFFSET = 0;
   /** Size in bytes that is used for data. */
   static final short WRITABLE_SIZE = WRITABLE_SIZE_OFFSET;

   /** Hidden default constructor. */
   protected HashDirectoryPage() {
      throw new AssertionError();
   }

   /**
    * Sets the next page pointer.
    *
    * @param page
    *           the current page
    * @param next
    *           the next directory page
    */
   static void setNextPagePointer(final Page< ? extends HashDirectoryPage> page, final PageID next) {
      page.writePageID(NEXT_PAGE_POINTER_OFFSET, next);
   }

   /**
    * Gets the next directory page id.
    *
    * @param page
    *           the current page
    * @return the page id of the next directory page in the chain
    */
   static PageID getNextPagePointer(final Page< ? extends HashDirectoryPage> page) {
      return page.readPageID(NEXT_PAGE_POINTER_OFFSET);
   }

   /**
    * Write a {@link PageID} to the given page at the given position.
    *
    * @param page
    *           page to write on
    * @param pos
    *           position to write at
    * @param pageID
    *           pageID to write
    */
   static void writePageID(final Page< ? extends HashDirectoryPage> page, final int pos, final PageID pageID) {
      final int off = pos * PageID.BYTES;
      if (off + PageID.BYTES > page.readShort(WRITABLE_SIZE_OFFSET)) {
         throw new IllegalArgumentException("Position " + pos + " is out of range.");
      }
      page.writePageID(off, pageID);
   }

   /**
    * Read the {@link PageID} from the given page at the given position.
    * @param page page to read from
    * @param pos position to read at
    * @return the PageID entry at the given position
    */
   static PageID readPageID(final Page< ? extends HashDirectoryPage> page, final int pos) {
      final int off = pos * PageID.BYTES;
       if (off + PageID.BYTES > page.readShort(WRITABLE_SIZE_OFFSET)) {
          throw new IllegalArgumentException("Position " + pos + " is out of range.");
       }
       return page.readPageID(off);
    }

   /**
    * Write a {@link PageID} to the given page at the given position.
    *
    * @param page
    *           page to write on
    * @param pos
    *           position to write at
    * @param depth
    *           depth to write
    */
   static void writeDepth(final Page< ? extends HashDirectoryPage> page, final int pos, final byte depth) {
      final int writable = page.readShort(WRITABLE_SIZE_OFFSET);
      final int off = writable - (pos + 1) * Byte.BYTES;
      page.getData()[off] = depth;
   }

   /**
    * Read the {@link PageID} from the given page at the given position.
    * @param page page to read from
    * @param pos position to read at
    * @return the depth entry at the given position
    */
   static byte readDepth(final Page< ? extends HashDirectoryPage> page, final int pos) {
      final int writable = page.readShort(WRITABLE_SIZE_OFFSET);
      final int off = writable - (pos + 1) * Byte.BYTES;
      return page.getData()[off];
   }

   /**
    * Gets the writeable on the given page based on the given index.
    * @param page page to get writable size from
    * @return number of slots on the given page
    */
   static short getWritableSize(final Page< ? extends HashDirectoryPage> page) {
       return page.readShort(WRITABLE_SIZE_OFFSET);
   }

   /**
    * Initializes a newly allocated page as a directory page.
    *
    * @param bufferManager
    *           the buffer manager
    * @return the initialized page
    */
   static Page<HashDirectoryPage> newPage(final BufferManager bufferManager) {
      @SuppressWarnings("unchecked")
      final Page<HashDirectoryPage> dirPage = (Page<HashDirectoryPage>) bufferManager.newPage();
      return initPage(dirPage);
   }

   /**
    * Initializes a newly allocated page as a directory page.
    *
    * @param dirPage
    *           the directory page
    * @return the initialized page
    */
   static Page<HashDirectoryPage> initPage(final Page<HashDirectoryPage> dirPage) {
      HashDirectoryPage.setNextPagePointer(dirPage, PageID.INVALID);
      dirPage.writeShort(WRITABLE_SIZE_OFFSET, WRITABLE_SIZE);
      Arrays.fill(dirPage.getData(), 0, WRITABLE_SIZE - 1, (byte) 0xff);
      return dirPage;
   }
}

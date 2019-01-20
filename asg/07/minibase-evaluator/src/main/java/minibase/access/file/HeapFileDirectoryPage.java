/*
 * @(#)HeapFileDirectoryPage.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import minibase.RecordID;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.file.DiskManager;

/**
 * A heap file directory page; contains DirEntry records.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 * @version 1.1
 */
final class HeapFileDirectoryPage extends HeapFilePage {

   /** The size of a directory entry. */
   private static final int ENTRY_SIZE = 8;

   /** Relative offset of a page id from an index. */
   private static final int IX_PAGE_ID = 0;

   /** Relative offset of a record count from an index. */
   private static final int IX_REC_CNT = 4;

   /** Relative offset of a free count from an index. */
   private static final int IX_FREE_CNT = 6;

   /** The size of the footer data (in bytes). */
   private static final int FOOTER_SIZE = 2;

   /** Page offset for the total number of entries. */
   private static final int ENTRY_COUNT = DiskManager.PAGE_SIZE - 2;

   /** Maximum number of entries in a directory page. */
   static final int MAX_ENTRIES = (DiskManager.PAGE_SIZE - HeapFilePage.HEADER_SIZE - FOOTER_SIZE)
         / ENTRY_SIZE;

   /** The amount of bytes reserved for content. */
   static final int DIR_CONTENT_SIZE = DiskManager.PAGE_SIZE - HEADER_SIZE - FOOTER_SIZE;

   /** Hidden default constructor. */
   private HeapFileDirectoryPage() {
      throw new AssertionError();
   }

   /**
    * Allocates and initializes a new page as a heap file directory page.
    *
    * @param bufferManager
    *           the buffer manager for getting the new page
    * @return the initialized page
    */
   public static Page<HeapFileDirectoryPage> newPage(final BufferManager bufferManager) {
      @SuppressWarnings("unchecked")
      final Page<HeapFileDirectoryPage> dirPage = (Page<HeapFileDirectoryPage>) (Page< ? >) HeapFilePage
            .newPage(bufferManager, HeapFile.DIR_PAGE);
      setEntryCount(dirPage, (short) 0);

      // initialize all page pointers
      for (int i = 0; i < MAX_ENTRIES; i++) {
         setPageID(dirPage, i, PageID.INVALID);
      }
      return dirPage;
   }

   /**
    * Gets the number of directory entries on the page.
    *
    * @param page
    *           the directory page
    * @return the entry count
    */
   static short getEntryCount(final Page<HeapFileDirectoryPage> page) {
      return page.readShort(HeapFileDirectoryPage.ENTRY_COUNT);
   }

   /**
    * Sets the number of directory entries on the page.
    *
    * @param page
    *           the directory page
    * @param entryCnt
    *           the new entry count
    */
   static void setEntryCount(final Page<HeapFileDirectoryPage> page, final short entryCnt) {
      page.writeShort(HeapFileDirectoryPage.ENTRY_COUNT, entryCnt);
   }

   /**
    * Gets the PageID at the given index.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    * @return the page ID
    */
   static PageID getPageID(final Page<HeapFileDirectoryPage> page, final int slotNo) {
      return page.readPageID(getSlotPos(slotNo) + HeapFileDirectoryPage.IX_PAGE_ID);
   }

   /**
    * Sets the PageID at the given index.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    * @param pageID
    *           ID to set
    */
   private static void setPageID(final Page<HeapFileDirectoryPage> page, final int slotNo,
         final PageID pageID) {
      page.writePageID(getSlotPos(slotNo) + HeapFileDirectoryPage.IX_PAGE_ID, pageID);
   }

   /**
    * Gets the page ID at the given index.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of data page
    * @return the page ID, or {@link PageID#INVALID} if it is not on this page
    */
   static PageID getPageID(final Page<HeapFileDirectoryPage> page, final RecordID rid) {
      // get and validate the record information
      if (checkRID(page, rid)) {
         return getPageID(page, rid.getSlotNo());
      }
      return PageID.INVALID;
   }

   /**
    * Sets the PageID at the given index.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of data page
    * @param pageID
    *           ID to set
    * @throws IllegalArgumentException if the record ID is not on this page
    */
   static void setPageID(final Page<HeapFileDirectoryPage> page, final RecordID rid, final PageID pageID) {
      if (!checkRID(page, rid)) {
         throw new IllegalArgumentException("Record ID is not valid for this page.");
      }
      setPageID(page, rid.getSlotNo(), pageID);
   }

   /**
    * Gets the record count at the given index.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    * @return the record count
    */
   private static short getRecordCount(final Page<HeapFileDirectoryPage> page, final int slotNo) {
      return page.readShort(getSlotPos(slotNo) + HeapFileDirectoryPage.IX_REC_CNT);
   }

   /**
    * Sets the record count at the given index.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    * @param recCnt
    *           new record count
    */
   private static void setRecordCount(final Page<HeapFileDirectoryPage> page, final int slotNo,
         final short recCnt) {
      page.writeShort(getSlotPos(slotNo) + HeapFileDirectoryPage.IX_REC_CNT, recCnt);
   }

   /**
    * Gets the record count of the given index.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of data page
    * @return the record count
    */
   static short getRecordCount(final Page<HeapFileDirectoryPage> page, final RecordID rid) {
      if (checkRID(page, rid)) {
         return getRecordCount(page, rid.getSlotNo());
      }
      throw new IllegalStateException();
   }

   /**
    * Sets the record count at the given index.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of data page
    * @param recCnt
    *           new record count
    */
   static void setRecordCount(final Page<HeapFileDirectoryPage> page, final RecordID rid,
         final short recCnt) {
      if (checkRID(page, rid)) {
         setRecordCount(page, rid.getSlotNo(), recCnt);
      }
   }

   /**
    * Gets the free count at the given index.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    * @return the free count
    */
   private static short getFreeCount(final Page<HeapFileDirectoryPage> page, final int slotNo) {
      return page.readShort(getSlotPos(slotNo) + HeapFileDirectoryPage.IX_FREE_CNT);
   }

   /**
    * Sets the free count at the given index.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    * @param freeCnt
    *           the new free count
    */
   private static void setFreeCount(final Page<HeapFileDirectoryPage> page, final int slotNo,
         final short freeCnt) {
      page.writeShort(getSlotPos(slotNo) + HeapFileDirectoryPage.IX_FREE_CNT, freeCnt);
   }

   /**
    * Gets the free count at the given index.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of data page
    * @return the free count
    */
   static short getFreeCount(final Page<HeapFileDirectoryPage> page, final RecordID rid) {
      if (checkRID(page, rid)) {
         return getFreeCount(page, rid.getSlotNo());
      }
      throw new IllegalStateException();
   }

   /**
    * Sets the free count at the given index.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of data page
    * @param freeCnt
    *           the new free count
    */
   static void setFreeCount(final Page<HeapFileDirectoryPage> page, final RecordID rid, final short freeCnt) {
      if (checkRID(page, rid)) {
         setFreeCount(page, rid.getSlotNo(), freeCnt);
      }
   }

   /**
    * Logically deletes an entry at the given slot number by shifting any successive entries down.
    *
    * @param page
    *           the directory page
    * @param slotNo
    *           slot number
    */
   private static void compact(final Page<HeapFileDirectoryPage> page, final int slotNo) {
      // shift all bytes to the left
      final int entryPos = getSlotPos(slotNo);
      final int succLen = DiskManager.PAGE_SIZE - FOOTER_SIZE - entryPos - ENTRY_SIZE;
      System.arraycopy(page.getData(), entryPos + ENTRY_SIZE, page.getData(), entryPos, succLen);
   }

   /**
    * Logically deletes an entry at the given slot number by shifting any successive entries down.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of the data page
    */
   static void compact(final Page<HeapFileDirectoryPage> page, final RecordID rid) {
      if (checkRID(page, rid)) {
         compact(page, rid.getSlotNo());
      }
   }

   /**
    * Logically deletes an entry at the given slot number.
    *
    * @param page
    *           the directory page
    * @param rid
    *           record id of the data page
    */
   static void deleteReference(final Page<HeapFileDirectoryPage> page, final RecordID rid) {
      if (checkRID(page, rid)) {
         page.writePageID(getSlotPos(rid.getSlotNo()) + HeapFileDirectoryPage.IX_PAGE_ID, PageID.INVALID);
      }
   }

   /**
    * Computes the offset of the given entry slot.
    *
    * @param slotNo
    *           the slot number
    * @return the offset
    */
   private static int getSlotPos(final int slotNo) {
      return HeapFilePage.HEADER_SIZE + slotNo * ENTRY_SIZE;
   }

   /**
    * Validates a record id exists on this page.
    *
    * @param page
    *           the page
    * @param rid
    *           the record ID to check
    * @return the record length (if valid)
    * @throws IllegalArgumentException
    *            if the slot is empty or the RID is invalid
    */
   private static boolean checkRID(final Page< ? extends HeapFileDirectoryPage> page, final RecordID rid) {
      // validate the record id
      if (!rid.getPageID().equals(page.getPageID()) || rid.getSlotNo() < 0 || rid.getSlotNo() > MAX_ENTRIES) {
         return false;
      }
      return true;
   }
}

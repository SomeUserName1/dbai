/*
 * @(#)HeapFilePage.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import minibase.RecordID;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.PageType;
import minibase.storage.file.DiskManager;

/**
 * Heap file data pages are implemented as slotted pages, with the slots at the front and the records in the
 * back, both growing into the free space in the middle of the page. This design assumes that records are kept
 * compacted when deletions are performed. Each slot contains the length and offset of its corresponding
 * record.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 * @version 1.1
 */
abstract class HeapFilePage implements PageType {

   /** Length of an "empty" slot in a heap file page. */
   private static final int EMPTY_SLOT = -1;

   /** Offset of the number of slots. */
   private static final int SLOT_CNT = 0;

   /** Offset of the used space offset. */
   private static final int USED_PTR = 2;

   /** Offset of the amount of free space. */
   private static final int FREE_SPACE = 4;

   /** Offset of the arbitrary page type. */
   private static final int PAGE_TYPE = 6;

   /** Offset of the previous page id. */
   private static final int PREV_PAGE = 8;

   /** Offset of the next page id. */
   private static final int NEXT_PAGE = 12;

   /** Total size of the header fields. */
   protected static final int HEADER_SIZE = 16;

   /** Size of a record slot. */
   static final int SLOT_SIZE = 4;

   /** The size of the footer data (in bytes). */
   private static final int FOOTER_SIZE = 4 + 2;

   /** The amount of bytes reserved for content. */
   static final int DATA_CONTENT_SIZE = DiskManager.PAGE_SIZE - HEADER_SIZE - FOOTER_SIZE;

   /** Offset of directory page id. */
   private static final int DIR_PAGE_ID = DiskManager.PAGE_SIZE - HeapFilePage.FOOTER_SIZE;

   /** Offset of directory slot number for this page. */
   private static final int DIR_SLOT_NO = DiskManager.PAGE_SIZE - 2;

   /** Hidden default constructor. */
   HeapFilePage() {
      throw new AssertionError();
   }

   /**
    * Initializes a newly allocated page as a heap file page.
    * 
    * @param bufferManager
    *           the buffer manager for getting the new page
    * @param pageType
    *           type of the heap file page
    * @return the initialized page
    */
   static Page<HeapFilePage> newPage(final BufferManager bufferManager, final short pageType) {
      return initPage(bufferManager.newPage(), pageType);
   }

   /**
    * Initializes a newly allocated page as a heap file page.
    * 
    * @param page
    *           the page to initialize as heap file page
    * @param pageType
    *           type of the heap file page
    * @return the initialized page
    */
   @SuppressWarnings("unchecked")
   static Page<HeapFilePage> initPage(final Page< ? > page, final short pageType) {
      // initially no slots in use
      page.writeShort(HeapFilePage.SLOT_CNT, (short) 0);

      // used offset grows backwards
      page.writeShort(HeapFilePage.USED_PTR, (short) (DiskManager.PAGE_SIZE - HeapFilePage.FOOTER_SIZE));

      // free space doesn't count headers and footer
      page.writeShort(HeapFilePage.FREE_SPACE, (short) (DiskManager.PAGE_SIZE - HeapFilePage.HEADER_SIZE
            - (pageType == HeapFile.DATA_PAGE ? HeapFilePage.FOOTER_SIZE : 0)));

      // optional type field may be used by sub classes
      page.writeShort(HeapFilePage.PAGE_TYPE, pageType);

      // set all page ids to invalid
      page.writePageID(HeapFilePage.PREV_PAGE, PageID.INVALID);
      page.writePageID(HeapFilePage.NEXT_PAGE, PageID.INVALID);
      return (Page<HeapFilePage>) page;
   }

   /**
    * Gets the number of slots on the page.
    * 
    * @param page
    *           the page
    * @return the slot count
    */
   static short getSlotCount(final Page<HeapFilePage> page) {
      return page.readShort(HeapFilePage.SLOT_CNT);
   }

   /**
    * Gets the amount of free space (in bytes).
    * 
    * @param page
    *           the page
    * @return the amount of free space
    */
   static short getFreeSpace(final Page<HeapFilePage> page) {
      return page.readShort(HeapFilePage.FREE_SPACE);
   }

   /**
    * Gets the arbitrary type of the page.
    * 
    * @param page
    *           the page
    * @return the page type
    */
   static short getType(final Page<HeapFilePage> page) {
      return page.readShort(HeapFilePage.PAGE_TYPE);
   }

   /**
    * Sets the arbitrary type of the page.
    * 
    * @param page
    *           the page
    * @param type
    *           the page type
    */
   static void setType(final Page<HeapFilePage> page, final short type) {
      page.writeShort(HeapFilePage.PAGE_TYPE, type);
   }

   /**
    * Gets the previous page's id.
    * 
    * @param page
    *           the page
    * @return the previous page's id
    */
   static PageID getPrevPage(final Page< ? extends HeapFilePage> page) {
      return page.readPageID(HeapFilePage.PREV_PAGE);
   }

   /**
    * Sets the previous page's id.
    * 
    * @param page
    *           the page
    * @param pageID
    *           the previous page's id
    */
   static void setPrevPage(final Page< ? extends HeapFilePage> page, final PageID pageID) {
      page.writePageID(HeapFilePage.PREV_PAGE, pageID);
   }

   /**
    * Gets the next page's id.
    * 
    * @param page
    *           the page
    * @return the next page's id
    */
   static PageID getNextPage(final Page< ? extends HeapFilePage> page) {
      return page.readPageID(HeapFilePage.NEXT_PAGE);
   }

   /**
    * Sets the next page's id.
    * 
    * @param page
    *           the page
    * @param pageID
    *           the next page's id
    */
   static void setNextPage(final Page< ? extends HeapFilePage> page, final PageID pageID) {
      page.writePageID(HeapFilePage.NEXT_PAGE, pageID);
   }

   /**
    * Gets the record id, which represents the directory page and the slot id pointing to this data page.
    * 
    * @param page
    *           the data page
    * @return record id of directory page
    */
   static RecordID getDirectoryRecordID(final Page< ? extends HeapFilePage> page) {
      return new RecordID(page.readPageID(HeapFilePage.DIR_PAGE_ID),
            page.readShort(HeapFilePage.DIR_SLOT_NO));
   }

   /**
    * Sets the record id, which represents the directory page and the slot id pointing to this data page.
    * 
    * @param page
    *           the data page
    * @param rid
    *           the record id of the directory
    */
   static void setDirectoryRecordID(final Page< ? extends HeapFilePage> page, final RecordID rid) {
      page.writePageID(HeapFilePage.DIR_PAGE_ID, rid.getPageID());
      page.writeShort(HeapFilePage.DIR_SLOT_NO, (short) rid.getSlotNo());
   }

   /**
    * Gets the length of the record referenced by the given slot.
    * 
    * @param page
    *           the page
    * @param slotno
    *           the length of the record referenced by the given slot
    * @return the slot's length
    */
   static short getSlotLength(final Page< ? extends HeapFilePage> page, final int slotno) {
      return page.readShort(HeapFilePage.HEADER_SIZE + slotno * HeapFilePage.SLOT_SIZE);
   }

   /**
    * Gets the offset of the record referenced by the given slot.
    * 
    * @param page
    *           the page
    * @param slotno
    *           the slot number
    * @return the offset of the record referenced by the given slot
    */
   private static short getSlotOffset(final Page< ? extends HeapFilePage> page, final int slotno) {
      return page.readShort(HeapFilePage.HEADER_SIZE + slotno * HeapFilePage.SLOT_SIZE + 2);
   }

   /**
    * Inserts a new record into the page.
    * 
    * @param page
    *           the page
    * @param record
    *           record to insert
    * @return RID of new record, or null if insufficient space
    */
   static RecordID insertRecord(final Page< ? extends HeapFilePage> page, final byte[] record) {
      // first check for sufficient space
      final short recLength = (short) record.length;
      final int spaceNeeded = recLength + HeapFilePage.SLOT_SIZE;
      short freeSpace = page.readShort(HeapFilePage.FREE_SPACE);
      if (spaceNeeded > freeSpace) {
         return null;
      }

      // linear search for an empty slot
      short slotCnt = page.readShort(HeapFilePage.SLOT_CNT);
      short i;
      int length;
      for (i = 0; i < slotCnt; i++) {
         length = getSlotLength(page, i);
         if (length == HeapFilePage.EMPTY_SLOT) {
            break;
         }
      }

      // if using a new slot
      if (i == slotCnt) {
         // adjust the free space
         freeSpace -= spaceNeeded;
         page.writeShort(HeapFilePage.FREE_SPACE, freeSpace);

         // adjust the slot count
         slotCnt++;
         page.writeShort(HeapFilePage.SLOT_CNT, slotCnt);
      } else {
         // otherwise, reusing an existing slot
         freeSpace -= recLength;
         page.writeShort(HeapFilePage.FREE_SPACE, freeSpace);
      }

      // update the used space offset
      short usedPtr = page.readShort(HeapFilePage.USED_PTR);
      usedPtr -= recLength;
      page.writeShort(HeapFilePage.USED_PTR, usedPtr);

      // update the slot, copy the record, and return the RID
      final int slotpos = HeapFilePage.HEADER_SIZE + i * HeapFilePage.SLOT_SIZE;
      page.writeShort(slotpos, recLength);
      page.writeShort(slotpos + 2, usedPtr);
      System.arraycopy(record, 0, page.getData(), usedPtr, recLength);
      return new RecordID(page.getPageID(), i);
   }

   /**
    * Selects a record from the page.
    *
    * @param page
    *           the page
    * @param rid
    *           the record ID
    * @return the corresponding record
    * @throws IllegalArgumentException
    *            if the rid is invalid
    */
   static byte[] selectRecord(final Page<? extends HeapFilePage> page, final RecordID rid) {
      // get and validate the record information
      final short length = checkRID(page, rid);
      final short offset = getSlotOffset(page, rid.getSlotNo());

      // finally, get and return the record
      final byte[] record = new byte[length];
      System.arraycopy(page.getData(), offset, record, 0, length);
      return record;
   }

   /**
    * Updates a record on the page.
    *
    * @param page
    *           the page
    * @param rid
    *           the record ID
    * @param record
    *           the new record data
    * @throws IllegalArgumentException
    *            if the rid or record size is invalid
    */
   static void updateRecord(final Page< ? extends HeapFilePage> page, final RecordID rid,
         final byte[] record) {
      // get and validate the record information
      final short length = checkRID(page, rid);
      if (record.length != length) {
         throw new IllegalArgumentException("Invalid record size");
      }

      // finally, update the record in place
      final short offset = getSlotOffset(page, rid.getSlotNo());
      System.arraycopy(record, 0, page.getData(), offset, length);
   }

   /**
    * Deletes a record from the page, compacting the records space. The slot directory cannot be compacted
    * because that would alter existing RIDs.
    *
    * @param page
    *           the page
    * @param rid
    *           the record ID
    * @throws IllegalArgumentException
    *            if the rid is invalid
    */
   static void deleteRecord(final Page< ? extends HeapFilePage> page, final RecordID rid) {
      // get and validate the record information
      final short length = checkRID(page, rid);
      final short offset = getSlotOffset(page, rid.getSlotNo());

      // calculate the compacting values
      final short usedPtr = page.readShort(HeapFilePage.USED_PTR);
      final short newSpot = (short) (usedPtr + length);
      final short size = (short) (offset - usedPtr);

      // shift all bytes to the right
      System.arraycopy(page.getData(), usedPtr, page.getData(), newSpot, size);

      // adjust offsets of all valid slots that refer
      // to the left of the record being removed
      final short slotCnt = page.readShort(HeapFilePage.SLOT_CNT);
      for (int i = 0, n = HeapFilePage.HEADER_SIZE; i < slotCnt; i++, n += HeapFilePage.SLOT_SIZE) {
         if (getSlotLength(page, i) != HeapFilePage.EMPTY_SLOT) {
            short chkoffset = getSlotOffset(page, i);
            if (chkoffset < offset) {
               chkoffset += length;
               page.writeShort(n + 2, chkoffset);
            }
         }
      }

      // move the used space offset forward
      page.writeShort(HeapFilePage.USED_PTR, newSpot);

      // increase freespace by size of hole
      short freeSpace = page.readShort(HeapFilePage.FREE_SPACE);
      freeSpace += length;
      page.writeShort(HeapFilePage.FREE_SPACE, freeSpace);

      // mark the slot as empty
      final int slotpos = HeapFilePage.HEADER_SIZE + rid.getSlotNo() * HeapFilePage.SLOT_SIZE;
      page.writeShort(slotpos, (short) HeapFilePage.EMPTY_SLOT);
      page.writeShort(slotpos + 2, (short) 0);
   }

   /**
    * Gets the RID of the first record on the page, or null if none.
    * 
    * @param page
    *           the page
    * @return the first record's ID
    */
   static RecordID firstRecord(final Page< ? extends HeapFilePage> page) {
      // find the first non-empty slot
      final short slotCnt = page.readShort(HeapFilePage.SLOT_CNT);
      int i = 0;
      for (; i < slotCnt; i++) {
         final short length = getSlotLength(page, i);
         if (length != HeapFilePage.EMPTY_SLOT) {
            break;
         }
      }

      // if all slots are empty, there are no records
      if (i == slotCnt) {
         return null;
      }

      // otherwise, found a non-empty slot
      return new RecordID(page.getPageID(), i);
   }

   /**
    * Gets the next RID after the given one, or null if no more.
    *
    * @param page
    *           the page
    * @param curRID
    *           the current record ID
    * @return next RID after the given one
    * @throws IllegalArgumentException
    *            if the rid is invalid
    */
   static RecordID nextRecord(final Page< ? extends HeapFilePage> page, final RecordID curRID) {
      // validate the record id
      final short slotCnt = page.readShort(HeapFilePage.SLOT_CNT);
      if (!curRID.getPageID().equals(page.getPageID()) || curRID.getSlotNo() < 0
            || curRID.getSlotNo() > slotCnt) {
         throw new IllegalArgumentException("Invalid RID");
      }

      // find the next non-empty slot
      int i = curRID.getSlotNo() + 1;
      for (; i < slotCnt; i++) {
         final short length = getSlotLength(page, i);
         if (length != HeapFilePage.EMPTY_SLOT) {
            break;
         }
      }

      // if remaining slots were empty, there are no more records
      if (i == slotCnt) {
         return null;
      }

      // otherwise, found a non-empty slot
      return new RecordID(page.getPageID(), i);
   }

   /**
    * Prints the contents of a heap file page.
    * 
    * @param page
    *           the page
    */
   static void print(final Page< ? extends HeapFilePage> page) {
      final short slotCnt = page.readShort(HeapFilePage.SLOT_CNT);

      System.out.println("HFPage:");
      System.out.println("-------");
      System.out.println("  prevPage  = " + page.readInt(HeapFilePage.PREV_PAGE));
      System.out.println("  nextPage  = " + page.readInt(HeapFilePage.NEXT_PAGE));
      System.out.println("  slotCnt   = " + slotCnt);
      System.out.println("  usedPtr   = " + page.readShort(HeapFilePage.USED_PTR));
      System.out.println("  freeSpace = " + page.readShort(HeapFilePage.FREE_SPACE));
      System.out.println("  pageType  = " + page.readShort(HeapFilePage.PAGE_TYPE));
      System.out.println("-------");

      for (int i = 0, n = HeapFilePage.HEADER_SIZE; i < slotCnt; i++, n += HeapFilePage.SLOT_SIZE) {
         System.out.println("slot #" + i + " offset = " + page.readShort(n));
         System.out.println("slot #" + i + " length = " + page.readShort(n + 2));
      }
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
   private static short checkRID(final Page< ? extends HeapFilePage> page, final RecordID rid) {
      // validate the record id
      final short slotCnt = page.readShort(HeapFilePage.SLOT_CNT);
      if (!rid.getPageID().equals(page.getPageID()) || rid.getSlotNo() < 0 || rid.getSlotNo() > slotCnt) {
         throw new IllegalArgumentException("Invalid RID");
      }

      // validate the record itself
      final short recLen = getSlotLength(page, rid.getSlotNo());
      if (recLen == HeapFilePage.EMPTY_SLOT) {
         throw new IllegalArgumentException("Empty slot");
      }
      return recLen;
   }
}

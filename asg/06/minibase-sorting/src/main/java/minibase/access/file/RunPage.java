/*
 * @(#)RunPage.java   1.0   Jan 10, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import java.util.Arrays;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.PageType;
import minibase.storage.buffer.UnpinMode;
import minibase.storage.file.DiskManager;


/**
 * Specialized page to be used by run that takes care of sequential storing order.
 *
 * @author Fabian Klopfer, Simon Suckut
 */
public final class RunPage implements PageType {
   /** Hidden default constructor. */
   private RunPage() {
      throw new AssertionError();
   }

   /**
    * Initializes a RunPage and sets the next pointer of the previous page accordingly.
    *
    * @param bufferManager
    *           the BufferManager used to get a Page for the RunPage and the previously used page
    * @param prevPageID
    *           previous RunPage's ID
    * @return
    *       the freshly initialized RunPage
    */
   @SuppressWarnings("unchecked")
   static Page<RunPage> newPage(final BufferManager bufferManager, final PageID prevPageID) {
      final Page<RunPage> newPage = (Page<RunPage>) bufferManager.newPage();
      RunPage.setNextPage(newPage, PageID.INVALID);
      if (prevPageID != PageID.INVALID) {
         final Page<RunPage> prevPage = bufferManager.pinPage(prevPageID);
         RunPage.setNextPage(prevPage, newPage.getPageID());
         bufferManager.unpinPage(prevPage, UnpinMode.DIRTY);
      }
      return newPage;
   }

   /**
    * Get the record stored at the given position as array of bytes.
    * 
    * @param page
    *           the page on which the desired record lives
    * @param position
    *           position of the record
    * @param recordLength
    *           length of the record
    * @return the record as array of bytes
    */
   static byte[] getTuple(final Page<RunPage> page, final int position, final int recordLength) {
      final int pageOffset = RunPage.tupleOffset(position, recordLength);
      return Arrays.copyOfRange(page.getData(), pageOffset, pageOffset + recordLength);
   }
   
   /**
    * Sets the record at the given position.
    * 
    * @param page
    *           the page to set the record on
    * @param pos
    *           position of the record
    * @param tuple
    *           record to set
    * @param recordLength
    *           length of the record to set
    */
   static void setTuple(final Page<RunPage> page, final int pos, final byte[] tuple, final int recordLength) {
      final int pageOffset = tupleOffset(pos, recordLength);
      final byte[] data = page.getData();
      System.arraycopy(tuple, 0, data, pageOffset, recordLength);
   }

   /**
    * Appends a record at the end of the given RunPage.
    * 
    * @param page
    *           the page to insert to
    * @param tuple
    *           record to append
    * @param numRecordsInRun
    *           number of records in the complete run by now
    * @param recordLength
    *           length of the record to insert
    * @param maxRecordsPerPage
    *           Maximum number of records in a page
    */
   static void insertEntry(final Page<RunPage> page, final byte[] tuple, final int numRecordsInRun,
         final int recordLength, final int maxRecordsPerPage) {
      final int pos = numRecordsInRun % maxRecordsPerPage;
      setTuple(page, pos, tuple, recordLength);
   } 

   /**
    * Gets the next-page ID stored in this page.
    * 
    * @param page
    *           the page
    * @return the previous-page ID
    */
   public static PageID getNextPage(final Page<RunPage> page) {
        // The NextPage pointer is located at the end of a page, thus page size - pageID size
       return page.readPageID(DiskManager.PAGE_SIZE - PageID.BYTES);
   } 

   /**
    * Sets the next-page ID of this page.
    * 
    * @param page
    *           the page
    * @param id
    *           new ID
    */
   private static void setNextPage(final Page<RunPage> page, final PageID id) {
      page.writePageID(DiskManager.PAGE_SIZE - PageID.BYTES, id);
   }

   /**
    * Calculates the offset of a given position.
    * 
    * @param pos
    *           Position to build the offset to
    * @param recordLength
    *           length of a record in the given page
    * @return the offset
    */
   private static int tupleOffset(final int pos, final int recordLength) {
      return pos * recordLength;
   }

   /**
    * Returns the maximum number of records in a RunPage given the recordLength.
    * 
    * @param recordLength
    *           length of each record in the page
    * @return Maximum number of records
    */
   static int maxNumberOfRecords(final int recordLength) {
      return (DiskManager.PAGE_SIZE - 4) / recordLength;
   }
}

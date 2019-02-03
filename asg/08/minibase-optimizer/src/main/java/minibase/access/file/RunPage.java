/*
 * @(#)RunPage.java   1.0   Jan 12, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.PageType;
import minibase.storage.file.DiskManager;

/**
 * Page type for the pages of a {@link Run} file.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class RunPage implements PageType {

   /** Hidden default constructor. */
   private RunPage() {
      throw new AssertionError();
   }

   /**
    * Initializes a page as a {@link RunPage}.
    *
    * @param page page to initialize
    * @return initialized run page
    */
   @SuppressWarnings("unchecked")
   public static Page<RunPage> initialize(final Page<?> page) {
      page.writePageID(DiskManager.PAGE_SIZE - PageID.BYTES, PageID.INVALID);
      return (Page<RunPage>) page;
   }

   /**
    * Sets the record at the given index on the given page.
    *
    * @param page page to set the record on
    * @param pos index of the record on the page
    * @param record record to set
    * @param recordLength length of the record
    */
   public static void setRecord(final Page<RunPage> page, final int pos, final byte[] record, final int recordLength) {
      System.arraycopy(record, 0, page.getData(), pos * recordLength, recordLength);
   }

   /**
    * Reads the record at the given index from the given run page.
    *
    * @param page page to read the record from
    * @param pos position of the record to read on the page
    * @param recordLength length of the record to read
    * @return the record
    */
   public static byte[] getRecord(final Page<RunPage> page, final int pos, final int recordLength) {
      final byte[] record = new byte[recordLength];
      System.arraycopy(page.getData(), pos * recordLength, record, 0, recordLength);
      return record;
   }

   /**
    * Sets the page ID of the next run page in the chain.
    *
    * @param page page to set the next page's ID on
    * @param nextPageID page ID to set
    */
   public static void setNextPageID(final Page<RunPage> page, final PageID nextPageID) {
      page.writePageID(DiskManager.PAGE_SIZE - PageID.BYTES, nextPageID);
   }

   /**
    * Reads the page ID of the next run page in the chain.
    *
    * @param page run page to read the next page's ID from
    * @return ID of the next run page in the chain
    */
   public static PageID getNextPageID(final Page<RunPage> page) {
      return page.readPageID(DiskManager.PAGE_SIZE - PageID.BYTES);
   }

   /**
    * Computes the maximum number of records of the given length that fit onto a run page.
    *
    * @param recordLength length of the records to store
    * @return maximum number of records that can be stored on one page
    */
   public static int capacity(final int recordLength) {
      return (DiskManager.PAGE_SIZE - PageID.BYTES) / recordLength;
   }
}

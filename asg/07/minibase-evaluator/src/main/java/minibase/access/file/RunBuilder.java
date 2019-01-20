/*
 * @(#)RunBuilder.java   1.0   Jan 12, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * A builder for {@link Run}s of same-size records.
 * The builder cannot be reused and only releases its resources when {@link #finish()} is called.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class RunBuilder {

   /** Buffer manager for allocating run pages. */
   private final BufferManager bufferManager;

   /** Length of the run's records. */
   private final int recordLength;

   /** Page ID of the run's first page. */
   private final PageID firstPageID;

   /** Page that is currently being filled. */
   private Page<RunPage> currentPage;

   /** Number of records already added to the run. */
   private long numRecords = 0;

   /**
    * Creates a run builder for records of the given length,  allocating with the given buffer manager.
    *
    * @param bufferManager buffer manager
    * @param recordLength length of the records
    */
   public RunBuilder(final BufferManager bufferManager, final int recordLength) {
      final Page<RunPage> firstPage = RunPage.initialize(bufferManager.newPage());
      this.bufferManager = bufferManager;
      this.recordLength = recordLength;
      this.firstPageID = firstPage.getPageID();
      this.currentPage = firstPage;
   }

   /**
    * Appends the given record to this builder's run.
    *
    * @param record record to append
    */
   public void appendRecord(final byte[] record) {
      if (this.currentPage == null) {
         throw new IllegalStateException("Builder has already been closed.");
      }
      final int cap = RunPage.capacity(this.recordLength);
      final int offset = (int) (this.numRecords % cap);
      if (offset == 0 && this.numRecords != 0) {
         final Page<RunPage> nextPage = RunPage.initialize(this.bufferManager.newPage());
         RunPage.setNextPageID(this.currentPage, nextPage.getPageID());
         this.bufferManager.unpinPage(this.currentPage, UnpinMode.DIRTY);
         this.currentPage = nextPage;
      }
      System.arraycopy(record, 0, this.currentPage.getData(), offset * this.recordLength, this.recordLength);
      this.numRecords++;
   }

   /**
    * Finished the run and releases all resources. This builder cannot be used after calling this method.
    *
    * @return the finished run
    */
   public Run finish() {
      this.bufferManager.unpinPage(this.currentPage, UnpinMode.DIRTY);
      this.currentPage = null;
      return new Run(this.firstPageID, this.numRecords);
   }
}

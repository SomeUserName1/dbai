/*
 * @(#)RunScan.java   1.0   Jan 12, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import java.util.NoSuchElementException;

import minibase.query.evaluator.TupleIterator;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * A sequential scan over a {@link Run}.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public class RunScan implements TupleIterator {
   /** Buffer manager. */
   private final BufferManager bufferManager;

   /** The run to iterate over. */
   private final Run run;

   /** Length of the records in the run. */
   private final int recordLength;

   /** Currently opened page of the run. */
   private Page<RunPage> currentPage;

   /** Position of this iterator inside the run. */
   private long pos = 0;

   /**
    * Creates a run scan that scans over the given run, reading records with the given length.
    *
    * @param bufferManager buffer manager
    * @param run run to iterate over
    * @param recordLength length of records in the given run
    */
   public RunScan(final BufferManager bufferManager, final Run run, final int recordLength) {
      this.bufferManager = bufferManager;
      this.run = run;
      this.recordLength = recordLength;
      this.currentPage = this.bufferManager.pinPage(run.getFirstPageID());
   }

   @Override
   public boolean hasNext() {
      return this.pos < this.run.getLength();
   }

   @Override
   public byte[] next() {
      if (this.currentPage == null) {
         throw new IllegalStateException("Iterator has already been closed.");
      }
      if (!this.hasNext()) {
         throw new NoSuchElementException();
      }

      final int capacity = RunPage.capacity(this.recordLength);
      final int offset = (int) (this.pos % capacity);
      if (offset == 0 && this.pos != 0) {
         final PageID nextID = RunPage.getNextPageID(this.currentPage);
         this.bufferManager.unpinPage(this.currentPage, UnpinMode.CLEAN);
         this.currentPage = this.bufferManager.pinPage(nextID);
      }

      final byte[] out = new byte[this.recordLength];
      System.arraycopy(this.currentPage.getData(), offset * this.recordLength, out, 0, this.recordLength);
      this.pos++;
      return out;
   }

   @Override
   public void reset() {
      if (this.currentPage == null) {
         throw new IllegalStateException("Iterator has already been closed.");
      }
      this.bufferManager.unpinPage(this.currentPage, UnpinMode.CLEAN);
      this.currentPage = this.bufferManager.pinPage(this.run.getFirstPageID());
      this.pos = 0;
   }

   @Override
   public void close() {
      if (this.currentPage != null) {
         this.bufferManager.unpinPage(this.currentPage, UnpinMode.CLEAN);
         this.currentPage = null;
         this.pos = -1;

         PageID currID = this.run.getFirstPageID();
         while (currID.isValid()) {
            final Page<RunPage> page = this.bufferManager.pinPage(currID);
            currID = RunPage.getNextPageID(page);
            this.bufferManager.freePage(page);
         }
      }
   }
}

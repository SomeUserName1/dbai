/*
 * @(#)ReplacementSortBuffer.java   1.0   Jan 10, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
// TODO merge into Sort or RunPage
package minibase.access.file;

import java.util.ArrayList;

import minibase.query.evaluator.TupleIterator;
import minibase.query.evaluator.compare.RecordComparator;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.file.DiskManager;

/**
 * Buffer as Iterator pinning all pages that it uses to react as a current set in the replacement sort.
 *
 * @author fabian.klopfer@uni-konstanz.de, simon.suckut@uni-konstanz.de
 * @version 1.0
 */
public class ReplacementSortBuffer implements TupleIterator {

   /** Buffer Manager. */
   private final BufferManager bufferManager;

   /** Number of Pages for the Buffer to use. */
   private final int maxPages;

   /** Pages for the buffer to use. */
   private final ArrayList<Page<RunPage>> pages;

   /** Length of each record in the buffer. */
   private final int recordLength;

   /** Indicating that the buffer is full (replacement is required */
   // private boolean full;

   /** How many records fit in one page. */
   private final int recordsPerPage;

   /** How many records fit in the buffer. */
   private final int maxTuplesInBuffer;

   /** How many records are currently in the buffer. */
   private int recordsInBuffer;

   /** The greatest by add returned record. */
   private byte[] greatestTuple;

   /** If this buffer is invalidated (no more usable). */
   private boolean invalidated;

   /** Compares records. */
   private final RecordComparator comparator;

   /** This buffers input. */
   private final TupleIterator input;

   /** Saves record from input if a new run had to be initialized. */
   private byte[] delayedTuple;

   /**
    * /** Instantiates a ReplacementSortBuffer with given buffer manager and a maximum number of pages which
    * will always be pinned.
    *
    * @param bufferManager
    *           to pin pages
    * @param maxPages
    *           number of pages in the buffer
    * @param recordLength
    *       length of a record
    *
    * @param comparator
    *       the comparator on whichs basis to sort
    * @param input
    *       the relation to be sorted
    */
   public ReplacementSortBuffer(final BufferManager bufferManager, final int maxPages, final int recordLength,
         final RecordComparator comparator, final TupleIterator input) {
      this.bufferManager = bufferManager;
      this.maxPages = maxPages;
      this.recordLength = recordLength;
      this.pages = new ArrayList<>(this.maxPages);
      // this.full = false;
      this.recordsPerPage = (DiskManager.PAGE_SIZE - 4) / this.recordLength;
      this.maxTuplesInBuffer = this.recordsPerPage * this.maxPages;
      this.greatestTuple = null;
      this.comparator = comparator;
      this.input = input;
      this.delayedTuple = null;

      for (this.recordsInBuffer = 0; this.recordsInBuffer < this.maxTuplesInBuffer
            && this.input.hasNext(); this.recordsInBuffer++) {
         // Fill buffer initially
         final int slotInPage = this.recordsInBuffer % recordsPerPage;
         if (slotInPage == 0) {
            // New page must be allocated
            this.pages.add(RunPage.newPage(this.bufferManager, PageID.INVALID));
         }
         final Page<RunPage> latestPage = this.pages.get(this.pages.size() - 1);
         RunPage.insertEntry(latestPage, this.input.next(), this.recordsInBuffer, this.recordLength,
               this.recordsPerPage);
      }
   }

   /**
    * Adds the given record to the buffer. If buffer is full, the smallest record greater than greatestTuple
    * will be replaced and this replaced record will be returned. (This can also be the input record).
    *
    * @return the next popped record
    */
   public byte[] next() {
      final byte[] record;
      if (this.delayedTuple != null) {
         record = this.delayedTuple;
         this.delayedTuple = null;
      } else {
         record = this.input.hasNext() ? this.input.next() : null;
      }
      this.invalidated();
      // Buffer is full. Find record to replace.
      // Initialize variables
      byte[] smallestSatisfyingTuple = null;
      int positionOfSmallestSatisfyingTuple = -1;

      // Iterate over all entries in the buffer to find smallest which is greater or equal to the smallest
      // ever returned.
      for (int i = 0; i < this.recordsInBuffer; i++) {
         // Find Tuple to compare
         final int slotInPage = i % recordsPerPage;
         final Page<RunPage> page = this.pages.get(i / this.recordsPerPage);
         final byte[] comparingTuple = RunPage.getTuple(page, slotInPage, this.recordLength);

         // Compare
         final boolean lessThan = smallestSatisfyingTuple == null
                 || this.comparator.lessThan(comparingTuple, smallestSatisfyingTuple);
         final boolean greaterOrEqualThan = this.greatestTuple == null
                 || this.comparator.greaterThanOrEqual(comparingTuple, this.greatestTuple);
         // Set
         if (lessThan && greaterOrEqualThan) {
            smallestSatisfyingTuple = comparingTuple;
            positionOfSmallestSatisfyingTuple = i;
         }
      }

      if (smallestSatisfyingTuple == null) {
         // No satisfying record could be found. Try the input record
         if ((this.greatestTuple == null || record == null)
                 || this.comparator.greaterThanOrEqual(record, this.greatestTuple)) {
            this.greatestTuple = record;
            return record;
         } else {
            // No satisfying element could be found
            this.delayedTuple = record;
            return null;
         }
      }
      // Check if the to add record is the greatest and return it immediately
      if (record != null) {
         if (this.comparator.lessThan(record, smallestSatisfyingTuple) && (this.greatestTuple == null
                 || this.comparator.greaterThanOrEqual(record, this.greatestTuple))) {
            this.greatestTuple = record;
            return record;
         }
      }

      // Replace smallestSatisfyingTuple with record and return smallestSatisfyingTupole
      this.greatestTuple = smallestSatisfyingTuple;
      final Page<RunPage> replacementPage = this.pages
            .get(positionOfSmallestSatisfyingTuple / this.recordsPerPage);
      if (record != null) {
         // Replacement possible. Replace.
         RunPage.setTuple(replacementPage, positionOfSmallestSatisfyingTuple % this.recordsPerPage, record,
               this.recordLength);
      } else {
         // Replacment not possible. Move records one further.
         this.recordsInBuffer--;
         for (int i = positionOfSmallestSatisfyingTuple; i < this.recordsInBuffer; i++) {
            final Page<RunPage> page1 = this.pages.get(i / this.recordsPerPage);
            final int pageSlot1 = i % this.recordsPerPage;
            final Page<RunPage> page2 = this.pages.get((i + 1) / this.recordsPerPage);
            final int pageSlot2 = (i + 1) % this.recordsPerPage;
            RunPage.setTuple(page1, pageSlot1, RunPage.getTuple(page2, pageSlot2, this.recordLength),
                  this.recordLength);
         }
      }
      return smallestSatisfyingTuple;
   }

    /**
     * Merge me somewhere, please.
     */
   public void newRun() {
      this.greatestTuple = null;
   }

   /**
    * Returns true if this scan was not closed before. Otherwise false.
    */
   private void invalidated() {
      if (this.invalidated) {
         throw new IllegalStateException("This run scan was already closed");
      }
   }

   @Override
   public boolean hasNext() {
      return this.recordsInBuffer > 0;
   }

   @Override
   public void reset() {
      // TODO implement TupleIterator.reset
   }

   @Override
   public void close() {
      this.invalidated();
      this.invalidated = true;
      for (Page<RunPage> runPage : this.pages) {
         this.bufferManager.freePage(runPage);
      }
   }
}

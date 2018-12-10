/*
 * @(#)BucketChainIterator.java   1.0   Nov 16, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.util.Iterator;
import java.util.NoSuchElementException;

import minibase.access.index.DataEntry;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * Iterator going through a bucket.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni-konstanz.de&gt;
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 */
public class BucketChainIterator implements Iterator<DataEntry> {

   /** Buffer manager. */
   private final BufferManager bufferManager;
   /** Size of an entry stored in the index. */
   private final int entrySize;

   /** Bucket page we are currently scanning for entries. */
   private PageID currPageId;
   /** Current entry, which is always valid unless there are no more elements. */
   private DataEntry currEntry;
   /** Slot position on the current page. */
   private int currPos;

   /**
    * Constructor.
    * 
    * @param bufferManager
    *              buffer manager
    * @param headId
    *              page id of the first bucket page
    * @param entrySize
    *              size of an data entry
    */
   public BucketChainIterator(final BufferManager bufferManager, final PageID headId, final int entrySize) {
      this.bufferManager = bufferManager;
      this.entrySize = entrySize;
      this.currPageId = headId;
      this.moveToNext();
   }
   
   /**
    * Advances the pointer to the next valid data entry or {@code null} if there are no more valid entries.
    */
   private void moveToNext() {
      Page<BucketPage> page = this.bufferManager.pinPage(this.currPageId);
      // no next page to scan, so we are done
      this.currEntry = null;

      // start at the current pos on the current page
      while (page != null) {
         // scan the current page
         while (this.currPos < BucketPage.getNumEntries(page)) {
            final DataEntry e = BucketPage.readDataEntryAt(page, this.currPos, this.entrySize);
            ++this.currPos;
            this.currEntry = e;
            this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
            return;
         }

         // advance to the next page
         this.currPageId = BucketPage.getNextPagePointer(page);
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
         page = this.currPageId.isValid() ? this.bufferManager.pinPage(this.currPageId) : null;
         this.currPos = 0;
      }
   }
   
   @Override
   public boolean hasNext() {
      return this.currEntry != null;
   }

   @Override
   public DataEntry next() {
      if (this.currEntry == null) {
         throw new NoSuchElementException("No more elements.");
      }
      final DataEntry next = this.currEntry;
      this.moveToNext();
      return next;
   }
}


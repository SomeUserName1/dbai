/*
 * @(#)KeyScan.java   1.0   Oct 13, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.util.NoSuchElementException;

import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.access.index.IndexEntry;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * Scans a {@link HashIndex} for {@link IndexEntry data entries} with a specific {@link SearchKey}. The
 * scan can only be opened through the {@link HashIndex#openScan(SearchKey)} method.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 */
public final class KeyScan implements IndexScan {

   /** Search key to scan for. */
   private final SearchKey searchKey;
   /** Buffer manager. */
   private final BufferManager bufferManager;
   /** Size of an entry stored in the index. */
   private final SearchKeyType keyType;

   /** Bucket page we are currently scanning for entries. */
   private Page<BucketPage> currPage;
   /** Current entry, which is always valid unless there are no more elements. */
   private IndexEntry currEntry;
   /** Slot position on the current page. */
   private int currPos;


   /**
    * Constructor.
    *
    * @param bufferManager
    *           bufferManager used
    * @param key
    *           search key to scan for
    * @param bucketID
    *           {@link PageID} of the first page of the bucket to scan
    * @param type
    *           the type of the search keys
    */
   KeyScan(final BufferManager bufferManager, final SearchKey key, final PageID bucketID, final SearchKeyType type) {
      this.bufferManager = bufferManager;
      this.searchKey = key;
      this.keyType = type;
      this.currEntry = null;
      this.currPos = 0;
      if (bucketID.isValid()) {
         this.currPage = bufferManager.pinPage(bucketID);
         this.moveToNext();
      }
   }

   /**
    * Advances the pointer to the next valid data entry or {@code null} if there are no more valid entries.
    */
   private void moveToNext() {
      // no next page to scan, so we are done
      this.currEntry = null;

      // start at the current pos on the current page
      while (this.currPage != null) {
         // scan the current page
         while (this.currPos < BucketPage.getNumEntries(this.currPage)) {
            final IndexEntry e = BucketPage.readDataEntryAt(this.currPage, this.currPos, this.keyType);
            ++this.currPos;
            if (this.searchKey.equals(e.getSearchKey())) {
               this.currEntry = e;
               return;
            }
         }

         // advance to the next page
         final PageID nextID = BucketPage.getNextPagePointer(this.currPage);
         this.bufferManager.unpinPage(this.currPage, UnpinMode.CLEAN);
         this.currPage = nextID.isValid() ? this.bufferManager.pinPage(nextID) : null;
         this.currPos = 0;
      }
   }

   @Override
   public boolean hasNext() {
      return this.currEntry != null;
   }

   @Override
   public IndexEntry next() {
      if (this.currEntry == null) {
         throw new NoSuchElementException("No more elements.");
      }
      final IndexEntry next = this.currEntry;
      this.moveToNext();
      return next;
   }

   @Override
   public void close() {
      if (this.currPage != null) {
         this.bufferManager.unpinPage(this.currPage, UnpinMode.CLEAN);
         this.currPage = null;
      }
   }
}

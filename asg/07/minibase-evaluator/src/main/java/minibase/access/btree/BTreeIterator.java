/*
 * @(#)BTreeIterator.java   1.0   Oct 23, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.btree;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.access.index.IndexEntry;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * An iterator traversing the {@link BTreeIndex} in ascending order, starting at a given
 * position.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class BTreeIterator implements IndexScan {

   /** The buffer manager of the {@link BTreeIndex}. */
   private final BufferManager bufferManager;
   /** The page ID of the start page, needed for {@link #restart()}. */
   private final PageID startPageID;
   /** The starting position, needed for {@link #restart()}. */
   private final int startPosition;
   /** The current page. */
   private Page<BTreeLeaf> page;
   /** The current position in the page. */
   private int position;
   /** Number of entries in the current page. */
   private int size;
   /** Type of the key in byte. */
   private final SearchKeyType keyType;

   /**
    * Constructor specifying the starting position of the iterator.
    *
    * @param bufferManager
    *           the buffer manager for (un)pinning pages
    * @param page
    *           the starting page (must be a leaf page)
    * @param startPos
    *           the starting position inside the page
    * @param keyType
    *           the type of the key in byte
    */
   public BTreeIterator(final BufferManager bufferManager, final Page<BTreeLeaf> page,
         final int startPos, final SearchKeyType keyType) {
      this.bufferManager = bufferManager;
      this.keyType = keyType;
      this.startPageID = page.getPageID();
      this.startPosition = startPos;

      this.page = page;
      this.position = startPos;
      this.size = BTreePage.getNumKeys(page);

      this.moveToNext();
   }

   @Override
   public boolean hasNext() {
      return this.position < this.size;
   }

   /**
    * Returns the next entry (i.e. pair of search key and record ID) in the iteration.
    *
    * @return the next entry
    */
   public IndexEntry next() {
      final int pos = this.position++;
      final SearchKey key = BTreeLeaf.getKey(this.page, pos, this.keyType);
      final RecordID recordID = BTreeLeaf.getRecordID(this.page, pos, this.keyType.getKeyLength());
      this.moveToNext();
      return new IndexEntry(key, recordID, this.keyType);
   }

   /**
    * Restarts this iterator from its initial position.
    */
   public void restart() {
      if (this.page != null) {
         this.bufferManager.unpinPage(this.page, UnpinMode.CLEAN);
      }
      this.page = this.bufferManager.pinPage(this.startPageID);
      this.position = this.startPosition;
      this.size = BTreePage.getNumKeys(this.page);
      this.moveToNext();
   }

   @Override
   public void close() {
      if (this.page != null) {
         this.bufferManager.unpinPage(this.page, UnpinMode.CLEAN);
         this.page = null;
      }
   }

   /**
    * Moves this iterator's position to the next entry.
    */
   private void moveToNext() {
      if (this.position >= this.size) {
         final PageID nextID = BTreeLeaf.getNextPage(this.page);
         this.bufferManager.unpinPage(this.page, UnpinMode.CLEAN);
         if (nextID.isValid()) {
            this.page = this.bufferManager.pinPage(nextID);
            this.position = 0;
            this.size = BTreePage.getNumKeys(this.page);
         } else {
            this.page = null;
         }
      }
   }

   @Override
   @Deprecated
   public void remove() {
      throw new UnsupportedOperationException();
   }
}

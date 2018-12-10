/*
 * @(#)HashIndexScan.java   1.0   Nov 12, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import minibase.SearchKey;
import minibase.access.index.DataEntry;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.PageID;

/**
 * The hash index scan scans the entire index and returns all {@link DataEntry data entries} contained in the
 * index.
 *
 * The HashIndex scan uses an index implementation-specific DirectoryIterator to iterate over the index and get 
 * PageIDs of the heads of the bucket chains. The
 * bucket chains are iterated using a BucketChainIterator that starts at the given PageID and returns
 * DataEntries until it reaches the end of the chain.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
 * @author Manfred Schaefer &lt;manfred.schaefer@uni-konstanz.de&gt
 */
public class HashIndexScan implements IndexScan {

   /** Size of an data entry. */
   private int entrySize;
   /** Iterator returning the primary pages of the index. */
   private Iterator<PageID> dir;
   /** Current bucket iterator. */
   private BucketChainIterator bucket;
   /** The buffer manager. */
   private BufferManager bufferManager;
   /** The current data entry. */
   private DataEntry currEntry;
   /** The last data entry. */
   private DataEntry lastEntry;
   
   /**
    * Constructor.
    * @param index
    *           index to scan over
    * @param bufferManager
    *           buffer manager
    * @param entrySize
    *           size of a data entry
    */
   HashIndexScan(final HashIndex index, final BufferManager bufferManager, final int entrySize) {
      this.bufferManager = bufferManager;
      this.entrySize = entrySize;
      this.dir = index.directoryIterator();
      this.bucket = new BucketChainIterator(bufferManager, this.dir.next(), entrySize);
      this.currHash = 0;
      this.moveToNext();
   }

   /**
    * Advances the pointer to the next entry that can be returned or null if no such entry exists.
    */
   private void moveToNext() {
      if (this.bucket.hasNext()) {
         this.currEntry = this.bucket.next();
         return;
      }
      while (this.dir.hasNext()) {
         this.bucket = new BucketChainIterator(this.bufferManager, this.dir.next(), this.entrySize);
         this.currHash++;
         if (this.bucket.hasNext()) {
            this.currEntry = this.bucket.next();
            return;
         }
      }
      this.currEntry = null;
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
      this.lastEntry = this.currEntry;
      this.moveToNext();
      return this.lastEntry;
   }

   @Override
   public void close() throws IOException {
      //No op.
   }

   // For legacy purposes to support hash join
   
   /** Current hash. */
   private int currHash;
   
   /**
    * Gets the key of the last record identifier returned.
    *
    * @return the key of the last returned record identifier
    */
   public SearchKey getLastKey() {
      return this.lastEntry.getSearchKey();
   }

   /**
    * Returns the hash value for the bucket containing the next record identifier or the
    * maximum number of buckets, if none.
    *
    * @return the has value of the bucket containing the next record identifier or the
    *         maximum number of buckets, if none
    */
   public int getNextHash() {
      if (!this.hasNext()) {
         return -1;
      }
      return this.currHash;
   }
}


/*
 * @(#)StaticHashIndex.java   1.0   Nov 16, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */

package minibase.access.hash;

import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.access.index.DataEntry;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * This is a simple implementation of a static HashIndex.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
public final class StaticHashIndex implements HashIndex {

   /** Invalid Number. */
   private static final int INVALID = -1;

   /** Default depth if none is given. */
   private static final byte DEFAULT_DEPTH = 7;

   /** File name of the hash index, {@link Optional#empty} for temporary indexes. */
   private final Optional<String> fileName;

   /** First page id of the directory. */
   private PageID headID;

   /** Global depth of the hash index. */
   private final int depth;

   /** Number of buckets of the hash index. */
   private final int numBuckets;

   /** Number of Entries in the index. */
   private int numEntries;

   /** Reference to the buffer manager. */
   private final BufferManager bufferManager;

   /** Size of a {@link DataEntry} stored in the index. */
   private final int entrySize;

   /**
    * Opens an existing static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @return an existing linear hash index
    */
   public static StaticHashIndex openIndex(final BufferManager bufferManager, final String fileName) {
      final PageID headID = bufferManager.getDiskManager().getFileEntry(fileName);
      return new StaticHashIndex(bufferManager, Optional.of(fileName), headID);
   }

   /**
    * Creates a persistent static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @param depth
    *           depth of the index
    * @return an empty persistent linear hash index
    */
   public static StaticHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final int searchKeyLength, final byte depth) {
      return createIndex(bufferManager, Optional.of(fileName), searchKeyLength, depth);
   }

   /**
    * Creates a persistent static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty persistent linear hash index
    */
   public static StaticHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final int searchKeyLength) {
      return createIndex(bufferManager, Optional.of(fileName), searchKeyLength, DEFAULT_DEPTH);
   }

   /**
    * Creates a static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @param depth
    *           depth of the index
    * @return an empty persistent linear hash index
    */
   private static StaticHashIndex createIndex(final BufferManager bufferManager,
         final Optional<String> fileName, final int searchKeyLength, final byte depth) {
      final PageID headID = initializeHeaderPage(bufferManager, 1 << depth, searchKeyLength);
      inizializeDirectory(bufferManager, headID);
      fileName.ifPresent(x -> bufferManager.getDiskManager().addFileEntry(x, headID));
      return new StaticHashIndex(bufferManager, fileName, headID);
   }

   /**
    * Creates a temporary static hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param searchKeyLength
    *           maximum length of the search key
    * @param depth
    *           depth of the index
    * @return an empty temporary linear hash index
    */
   public static StaticHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final int searchKeyLength, final byte depth) {
      return createIndex(bufferManager, Optional.empty(), searchKeyLength, depth);
   }

   /**
    * Creates a temporary static hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty temporary linear hash index
    */
   public static StaticHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final int searchKeyLength) {
      return createIndex(bufferManager, Optional.empty(), searchKeyLength, DEFAULT_DEPTH);
   }

   /**
    * Initializes a page to be used as a header page.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param numBuckets
    *           minimum number of buckets
    * @param searchKeyLength
    *           maximum length of the search key
    * @return page id of the initialized header page
    */
   private static PageID initializeHeaderPage(final BufferManager bufferManager, final int numBuckets,
         final int searchKeyLength) {
      final Page<HashDirectoryHeader> header = HashDirectoryHeader.newPage(bufferManager, numBuckets,
            INVALID, INVALID, DataEntry.getLength(searchKeyLength));
      final PageID headID = header.getPageID();
      bufferManager.unpinPage(header, UnpinMode.DIRTY);
      return headID;
   }

   /**
    * Constructs a static hash index given an initialized header page id.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param headID
    *           page id of the header page for the index
    */
   private StaticHashIndex(final BufferManager bufferManager, final Optional<String> fileName,
         final PageID headID) {
      this.bufferManager = bufferManager;
      this.fileName = fileName;
      this.headID = headID;

      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      this.numEntries = HashDirectoryHeader.getSize(header);
      this.numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      this.entrySize = HashDirectoryHeader.getEntrySize(header);
      this.bufferManager.unpinPage(header, UnpinMode.CLEAN);
      this.depth = 31 - Integer.numberOfLeadingZeros(this.numBuckets);
   }

   /**
    * Initializes the directory of the index.
    * 
    * @param bufferManager
    *           buffer manager
    * @param headID
    *           first page id of the directory
    */
   private static void inizializeDirectory(final BufferManager bufferManager, final PageID headID) {
      final Page<HashDirectoryHeader> header = bufferManager.pinPage(headID);
      int numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      bufferManager.unpinPage(header, UnpinMode.CLEAN);
      Page<HashDirectoryPage> page = bufferManager.pinPage(headID);
      int pos = 0;
      while (numBuckets > 0) {
         if (pos >= StaticHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page))) {
            final Page<HashDirectoryPage> newPage = HashDirectoryPage.newPage(bufferManager);
            HashDirectoryPage.setNextPagePointer(page, newPage.getPageID());
            bufferManager.unpinPage(page, UnpinMode.DIRTY);
            page = newPage;
            pos = 0;
         }
         HashDirectoryPage.writePageID(page, pos, PageID.INVALID);
         --numBuckets;
         pos += PageID.SIZE;
      }
      bufferManager.unpinPage(page, UnpinMode.DIRTY);
   }

   /**
    * Retrieves bucket id from the given hash value.
    *
    * @param hash
    *           the hash value
    * @return the bucket id
    */
   PageID getBucketID(final int hash) {
      if (hash >= this.numBuckets) {
         throw new IllegalArgumentException();
      }
      // we start with the first bucket entries on the header page
      PageID pageID = this.headID;

      int bucket = hash;
      while (pageID.isValid()) {
         final Page<HashDirectoryPage> page = this.bufferManager.pinPage(pageID);
         pageID = HashDirectoryPage.getNextPagePointer(page);
         final int num = StaticHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
         if (bucket < num) {
            final PageID bucketID = HashDirectoryPage.readPageID(page, bucket);
            this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
            return bucketID;
         }
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
         // we have not found the bucket on the current page,
         // so proceed with a smaller offset for the next page
         bucket -= num;
      }

      // we have no bucket for the given hash
      throw new IllegalStateException();
   }

   /**
    * Sets the bucket id for the hash with the given bucket id.
    *
    * @param hash
    *           the hash value
    * @param bucketID
    *           the replacement bucket id
    */
   private void setBucketID(final int hash, final PageID bucketID) {
       if (hash >= this.numBuckets) {
           throw new IllegalArgumentException();
        }
        // we start with the first bucket entries on the header page
        PageID pageID = this.headID;

        int bucket = hash;
        while (pageID.isValid()) {
           final Page<HashDirectoryPage> page = this.bufferManager.pinPage(pageID);
           pageID = HashDirectoryPage.getNextPagePointer(page);
           final int num = StaticHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
           if (bucket < num) {
              HashDirectoryPage.writePageID(page, bucket, bucketID);
              this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
              return;
           }
           this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
           // we have not found the bucket on the current page,
           // so proceed with a smaller offset for the next page
           bucket -= num;
        }

        // we have no bucket for the given hash
        throw new IllegalStateException();
     }

   @Override
   public void delete() {
      PageID pageID = this.headID;
      while (pageID.isValid()) {
         final Page<HashDirectoryPage> page = this.bufferManager.pinPage(pageID);
         final int numEntries = StaticHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
         for (int i = 0; i < numEntries; i++) {
             this.deleteBucketChain(HashDirectoryPage.readPageID(page, i));
         }
         pageID = HashDirectoryPage.getNextPagePointer(page);
         this.bufferManager.freePage(page);
      }
      this.fileName.ifPresent(x -> this.bufferManager.getDiskManager().deleteFileEntry(x));
      this.headID = PageID.INVALID;
   }

   /**
    * Deletes the whole bucket starting with the page id of the primary bucket.
    *
    * @param bucketID
    *           the primary bucket
    */
   private void deleteBucketChain(final PageID bucketID) {
      PageID current = bucketID;
      while (current.isValid()) {
         final Page<BucketPage> page = this.bufferManager.pinPage(current);
         current = BucketPage.getNextPagePointer(page);
         this.bufferManager.freePage(page);
      }
   }

   @Override
   public Optional<DataEntry> search(final SearchKey key) {
      if (key == null) {
         throw new NullPointerException("SearchKey should not be `null`.");
      }

      try (IndexScan scan = this.openScan(key)) {
         if (scan.hasNext()) {
            final DataEntry result = scan.next();
            return Optional.of(result);
         }
      } catch (final IOException e) {
         throw new IOError(e);
      }
      return Optional.empty();
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(rid);
      if (!rid.getPageID().isValid()) {
         throw new IllegalArgumentException("The record ID's page ID must be valid.");
      }
      final DataEntry entry = new DataEntry(key, rid, this.entrySize);

      // search dataPage for key and insert
      final int hash = key.getHash(this.depth);
      final PageID dataID = this.getBucketID(hash);
      final PageID newDataID = this.insertIntoBucket(dataID, entry);
      if (!newDataID.equals(dataID)) {
         this.setBucketID(hash, newDataID);
      }
      this.numEntries += 1;
      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      HashDirectoryHeader.setSize(header, this.numEntries);
      this.bufferManager.unpinPage(header, UnpinMode.DIRTY);
   }

   /**
    * Tries to insert entry into bucket.
    * 
    * @param bucketHeadId
    *           the bucket to insert into
    * @param entry
    *           the entry to insert
    * @return whether the
    */
   private PageID insertIntoBucket(final PageID bucketHeadId, final DataEntry entry) {
      Page<BucketPage> page;
      if (!bucketHeadId.isValid()) {
         page = BucketPage.newPage(this.bufferManager);
      } else {
         page = this.bufferManager.pinPage(bucketHeadId);
      }

      if (BucketPage.hasSpaceLeft(page, this.entrySize)) {
         BucketPage.appendDataEntry(page, entry, this.entrySize);
         final PageID returnId = page.getPageID();
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
         return returnId;
      }
      this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
      page = BucketPage.newPage(this.bufferManager);
      final PageID newHead = page.getPageID();
      BucketPage.setOverflowPointer(page, bucketHeadId);
      BucketPage.appendDataEntry(page, entry, this.entrySize);
      this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
      return newHead;
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {
      final DataEntry entry = new DataEntry(Objects.requireNonNull(key), Objects.requireNonNull(rid),
            this.entrySize);
      if (this.remove(key.getHash(this.depth), entry)) {
         this.numEntries -= 1;
         final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
         HashDirectoryHeader.setSize(header, this.numEntries);
         this.bufferManager.unpinPage(header, UnpinMode.DIRTY);
         return true;
      }
      return false;
   }

   /**
    * Deletes the given entry from the bucket.
    *
    * @param bucketNo
    *           number of the bucket to delete from
    * @param entry
    *           entry to delete
    * @return {@code true} if the entry was deleted, {@code false} otherwise
    */
   private boolean remove(final int bucketNo, final DataEntry entry) {
      final PageID firstPageID = this.getBucketID(bucketNo);
      if (!firstPageID.isValid()) {
         return false;
      }
      final Page<BucketPage> primaryPage = this.bufferManager.pinPage(firstPageID);

      // iterate over bucket chain
      PageID deleteFromID = firstPageID;
      while (deleteFromID.isValid()) {
         final Page<BucketPage> deleteFrom = this.bufferManager.pinPage(deleteFromID);
         // iterate over every entry on the page
         for (int i = 0; i < BucketPage.getNumEntries(deleteFrom); ++i) {
            final DataEntry e = BucketPage.readDataEntryAt(deleteFrom, i, entry.getEntrySize());
            if (!entry.equals(e)) {
               continue;
            }
            // we have found our entry to remove
            if (deleteFromID.equals(firstPageID)) {
               // we are still on the primary page of the bucket
               BucketPage.removeDataEntryAt(deleteFrom, i, entry.getEntrySize());
            } else {
               // we are on an overflow page, need to delete the entry there and get a new entry
               // from the primary page so our invariant that overflow pages are always full still holds
               BucketPage.moveLastEntryTo(primaryPage, deleteFrom, i, entry.getEntrySize());
            }
            // the overflow page is now full again
            this.bufferManager.unpinPage(deleteFrom, UnpinMode.DIRTY);
            // check if primary page is now empty and chain needs to be shrunk
            if (BucketPage.getNumEntries(primaryPage) == 0) {
               final PageID nextID = BucketPage.getNextPagePointer(primaryPage);
               this.bufferManager.freePage(primaryPage);
               this.setBucketID(bucketNo, nextID);
            } else {
               // the primary page has still entries left
               this.bufferManager.unpinPage(primaryPage, UnpinMode.DIRTY);
            }
            return true;
         }
         // move along the chain
         deleteFromID = BucketPage.getNextPagePointer(deleteFrom);
         this.bufferManager.unpinPage(deleteFrom, UnpinMode.CLEAN);
      }
      this.bufferManager.unpinPage(primaryPage, UnpinMode.CLEAN);
      return false;
   }

   @Override
   public HashIndexScan openScan() {
      return new HashIndexScan(this, this.bufferManager, this.entrySize);
   }

   @Override
   public IndexScan openScan(final SearchKey key) {
      return new KeyScan(this.bufferManager, key, this.getBucketID(key.getHash(this.depth)), this.entrySize);
   }

   @Override
   public void close() {
      if (this.headID.isValid()) {
         // write meta-data onto header page
         final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
         HashDirectoryHeader.setNumberBuckets(header, this.numBuckets);
         HashDirectoryHeader.setSize(header, this.numEntries);
         this.bufferManager.unpinPage(header, UnpinMode.DIRTY);
         this.headID = PageID.INVALID;
      }
   }

   @Override
   public BufferManager getBufferManager() {
      return this.bufferManager;
   }

   @Override
   public int getDepth() {
      return this.depth;
   }

   @Override
   public PageID getHeadID() {
      return this.headID;
   }

   @Override
   public Iterator<PageID> directoryIterator() {
      return new HashDirectoryIterator(this.bufferManager, this.headID);
   }

   @Override
   public void printSummary() {
      // TODO Auto-generated method stub
   }
   
   /**
    * Calculates the maximum number of PageID's given the writable size.
    * @param writableSize writable size
    * @return number of Page ID's
    */
   private static int calcNumPageIDPerPage(final short writableSize) {
       return writableSize / PageID.SIZE;
   }
}

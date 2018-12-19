/*
 * @(#)HashIndex.java   1.0   Dec 2, 2014
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
import java.util.Optional;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.access.index.Index;
import minibase.access.index.IndexEntry;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * Abstract base class for hash based indices.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 * @author Leonard Woerteler &lt;leonard.woerteler@uni.kn&gt;
 */
public abstract class HashIndex implements Index {
   /** Buffer manager of pages used in this index. */
   private final BufferManager bufferManager;
   /** File name of the hash index, {@link Optional#empty} for temporary indexes. */
   private final Optional<String> fileName;
   /** Size of a {@link IndexEntry} stored in the index. */
   private final SearchKeyType keyType;

   /** Page ID of this index's {@link HashDirectoryHeader header page}. */
   private PageID headID;

   /** Number of buckets of the hash index. */
   private int numBuckets;
   /** Number of Entries in the index. */
   private int numEntries;

   /**
    * Constructs a hash index with the given parameters.
    *
    * @param bufferManager
    *           for managing this index's pages
    * @param fileName
    *           name of this index in the minibase directory, {@link Optional#empty()} for temporary index
    * @param headID
    *           page ID of the {@link HashDirectoryHeader directory header page}
    * @param numBuckets
    *           number of buckets of this index
    * @param numEntries
    *           number of entries in this index
    * @param type
    *           the type of the search keys
    */
   HashIndex(final BufferManager bufferManager, final Optional<String> fileName, final PageID headID,
         final int numBuckets, final int numEntries, final SearchKeyType type) {
      this.bufferManager = bufferManager;
      this.fileName = fileName;
      this.keyType = type;
      this.headID = headID;

      this.numBuckets = numBuckets;
      this.numEntries = numEntries;
   }

   /**
    * Gets the bucket number from a search key.
    *
    * @param key
    *           search key to derive the bucket number from
    * @return the bucket number corresponding to the given search key
    */
   abstract int getBucketNo(SearchKey key);

   /**
    * This method can be used to write additional metadata onto the header page before the index is closed.
    *
    * @param header
    *           pinned header page to write to
    */
   abstract void writeAdditionalMetadata(Page<HashDirectoryHeader> header);

   /**
    * Checks if the given directory entry represents the first occurrence of the bucket in the directory and if
    * the associated page ID is valid.
    *
    * @param hash
    *           hash value of the entries in the bucket
    * @param dirPage
    *           pined directory page the entry is on
    * @param offset
    *           offset of the entry on the directory page
    * @return {@code true} if the entry is a primary bucket with valid page ID, {@code false} otherwise
    */
   abstract boolean isValidPrimaryBucket(int hash, Page<HashDirectoryPage> dirPage, int offset);

   /**
    * Determines how many directory entries can be stored on the given directory page.
    *
    * @param dirPage
    *          directory page to determine the capacity of
    * @return number of directory entries that fit onto the given page
    */
   abstract int capacity(Page<? extends HashDirectoryPage> dirPage);

   /**
    * Creates an overview over the structure of this index. In contrast to {@link #toString()} this method <i>does</i>
    * use the {@link BufferManager}.
    *
    * @return overview string
    */
   public abstract String prettyPrint();

   @Override
   public final Optional<IndexEntry> search(final SearchKey key) {
      final BufferManager bufferManager = this.getBufferManager();
      PageID bucketID = this.getBucketID(this.getBucketNo(key));
      while (bucketID.isValid()) {
         // scan the current page
         final Page<BucketPage> page = bufferManager.pinPage(bucketID);
         final int onPage = BucketPage.getNumEntries(page);
         for (int pos = 0; pos < onPage; pos++) {
            if (key.equals(BucketPage.readSearchKey(page, pos, this.getKeyType()))) {
               final IndexEntry result = BucketPage.readDataEntryAt(page, pos, this.getKeyType());
               bufferManager.unpinPage(page, UnpinMode.CLEAN);
               return Optional.of(result);
            }
         }
         // advance to the next page
         bucketID = BucketPage.getNextPagePointer(page);
         bufferManager.unpinPage(page, UnpinMode.CLEAN);
      }
      return Optional.empty();
   }

   @Override
   public final IndexScan openScan(final SearchKey key) {
      return new KeyScan(this.getBufferManager(), key, this.getBucketID(this.getBucketNo(key)), this.getKeyType());
   }

   @Override
   public final IndexScan openScan() {
      final int dirSize = this.numBuckets;
      final BufferManager bufferManager = this.bufferManager;
      return new IndexScan() {
         private Page<HashDirectoryPage> dirPage = bufferManager.pinPage(HashIndex.this.headID);
         private int skipped = 0;
         private int dirPos = 0;
         private int onDirPage = Math.min(HashIndex.this.capacity(this.dirPage), dirSize);

         private Page<BucketPage> bucketPage = null;
         private int bucketPos = 0;
         private int onBucketPage = 0;

         private IndexEntry next = null;

         @Override
         public boolean hasNext() {
            if (this.next != null) {
               return true;
            }

            while (this.bucketPage == null) {
               final int hash = this.dirPos++;
               if (hash == dirSize) {
                  bufferManager.unpinPage(this.dirPage, UnpinMode.CLEAN);
                  this.dirPage = null;
                  return false;
               }

               int offset = hash - this.skipped;
               if (offset == this.onDirPage) {
                  final PageID nextID = HashDirectoryPage.getNextPagePointer(this.dirPage);
                  this.skipped += this.onDirPage;
                  bufferManager.unpinPage(this.dirPage, UnpinMode.CLEAN);
                  this.dirPage = bufferManager.pinPage(nextID);
                  offset = 0;
                  this.onDirPage = Math.min(HashIndex.this.capacity(this.dirPage), dirSize - this.skipped);
               }

               if (HashIndex.this.isValidPrimaryBucket(hash, this.dirPage, offset)) {
                  final PageID bucketID = HashDirectoryPage.readPageID(this.dirPage, offset);
                  final Page<BucketPage> bucket = bufferManager.pinPage(bucketID);
                  final int inBucket = BucketPage.getNumEntries(bucket);
                  if (inBucket > 0) {
                     this.bucketPage = bucket;
                     this.bucketPos = 0;
                     this.onBucketPage = inBucket;
                  } else {
                     bufferManager.unpinPage(bucket, UnpinMode.CLEAN);
                  }
               }
            }

            this.next = BucketPage.readDataEntryAt(this.bucketPage, this.bucketPos++, HashIndex.this.getKeyType());
            if (this.bucketPos == this.onBucketPage) {
               final PageID nextID = BucketPage.getNextPagePointer(this.bucketPage);
               bufferManager.unpinPage(this.bucketPage, UnpinMode.CLEAN);
               if (nextID.isValid()) {
                  this.bucketPage = bufferManager.pinPage(nextID);
                  this.bucketPos = 0;
                  this.onBucketPage = BucketPage.getNumEntries(this.bucketPage);
               } else {
                  this.bucketPage = null;
               }
            }

            return true;
         }

         @Override
         public IndexEntry next() {
            if (!this.hasNext()) {
               throw new NoSuchElementException();
            }
            final IndexEntry entry = this.next;
            this.next = null;
            return entry;
         }

         @Override
         public void close() {
            if (this.dirPage != null) {
               bufferManager.unpinPage(this.dirPage, UnpinMode.CLEAN);
               this.dirPage = null;
            }
         }
      };
   }

   @Override
   public final void close() {
      if (this.headID.isValid()) {
         final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
         HashDirectoryHeader.setNumberBuckets(header, this.numBuckets);
         HashDirectoryHeader.setSize(header, this.numEntries);
         this.writeAdditionalMetadata(header);
         this.bufferManager.unpinPage(header, UnpinMode.DIRTY);
         this.headID = PageID.INVALID;
      }
   }

   @Override
   public final void delete() {
      PageID dirID = this.headID;
      int hash = 0;
      for (int entriesLeft = this.getNumBuckets(), inPage; entriesLeft > 0; entriesLeft -= inPage) {
         final Page<HashDirectoryPage> dirPage = this.bufferManager.pinPage(dirID);
         inPage = Math.min(entriesLeft, this.capacity(dirPage));
         for (int pos = 0; pos < inPage; pos++) {
            // delete only the first occurrence of the bucket
            if (this.isValidPrimaryBucket(hash, dirPage, pos)) {
               PageID current = HashDirectoryPage.readPageID(dirPage, pos);
               do {
                  final Page<BucketPage> page = this.bufferManager.pinPage(current);
                  current = BucketPage.getNextPagePointer(page);
                  this.bufferManager.freePage(page);
               } while (current.isValid());
            }
            hash++;
         }
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         this.bufferManager.freePage(dirPage);
      }
      this.getFileName().ifPresent(x -> this.bufferManager.getDiskManager().deleteFileEntry(x));
      this.headID = PageID.INVALID;
   }

   @Override
   public final String toString() {
      final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName()).append('[');
      this.fileName.ifPresent(name -> sb.append("name='" + name + "', "));
      if (this.headID.isValid()) {
         sb.append("headID=").append(this.headID).append(", numBuckets=").append(this.numBuckets)
               .append(", numEntries=").append(this.numEntries)
               .append(", entrySize=").append(this.keyType.getKeyLength() + RecordID.BYTES);
      } else {
         sb.append("closed");
      }
      return sb.append(']').toString();
   }

   /**
    * Gets the bucket id of the bucket with the given number.
    *
    * @param bucketNo
    *           bucket number
    * @return PageID for associated bucket
    */
   final PageID getBucketID(final int bucketNo) {
      int offset = bucketNo;
      for (PageID pageID = this.headID; pageID.isValid();) {
         final Page<HashDirectoryPage> page = this.bufferManager.pinPage(pageID);
         pageID = HashDirectoryPage.getNextPagePointer(page);
         final int capacity = this.capacity(page);
         if (offset < capacity) {
            final PageID bucketID = HashDirectoryPage.readPageID(page, offset);
            this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
            return bucketID;
         }
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
         // we have not found the bucket on the current page, so proceed with a smaller offset for the next page
         offset -= capacity;
      }

      // we have no bucket for the given hash
      throw new IllegalStateException("Bucket number too big: " + bucketNo);
   }

   /**
    * Returns the page ID of the header page.
    *
    * @return page ID of the header page
    */
   final PageID getHeadID() {
      return this.headID;
   }

   /**
    * Returns this index's buffer manager.
    *
    * @return buffer manager
    */
   final BufferManager getBufferManager() {
      return this.bufferManager;
   }

   /**
    * Returns the size of entries in this index.
    *
    * @return size of index entries
    */
   final int getEntrySize() {
      return this.keyType.getKeyLength() + RecordID.BYTES;
   }

   /**
    * Returns the current number of buckets of this index.
    *
    * @return current number of buckets
    */
   final int getNumBuckets() {
      return this.numBuckets;
   }

   /**
    * Sets a new current number of buckets of this index.
    *
    * @param newNumBuckets
    *          new number of buckets
    */
   final void setNumBuckets(final int newNumBuckets) {
      this.numBuckets = newNumBuckets;
   }

   /**
    * Returns the current number of entries stored in this index.
    *
    * @return current number of entries
    */
   final int getNumEntries() {
      return this.numEntries;
   }

   /**
    * Adds the given difference to the current number of entries in this index.
    *
    * @param delta
    *          difference between old and new number of buckets
    */
   final void updateNumEntries(final int delta) {
      this.numEntries += delta;
   }

   /**
    * Returns the name of this index in the database directory.
    *
    * @return name of the index, or {@link Optional#empty()} if this index is temporary
    */
   final Optional<String> getFileName() {
      return this.fileName;
   }

   @Override
   public final SearchKeyType getKeyType() {
      return this.keyType;
   }

   /**
    * Returns the lower {@code n} bits of the given hash value.
    * @param hash hash value
    * @param n number of bits to preserve
    * @return an {@code int} containing the lowest {@code n} bits in {@code hash}
    */
   static final int nBits(final int hash, final int n) {
      return n <= 0 ? 0 : n < 32 ? hash & ((1 << n) - 1) : hash;
   }
}

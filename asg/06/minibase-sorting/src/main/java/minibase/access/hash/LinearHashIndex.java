/*
 * @(#)LinearHashIndex.java   1.0   Nov 26, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.util.Objects;
import java.util.Optional;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.access.index.IndexEntry;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * This unclustered index implements linear hashing as in the textbook (3rd edition).
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni-konstanz.de&gt;
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 */
public final class LinearHashIndex extends HashIndex {
   /** Minimum number of buckets that is preserved when shrinking the directory. */
   private static final int MIN_NUM_BUCKETS = 1;
   /** Default minimum load factor. */
   private static final float DEFAULT_MIN_LOAD_FACTOR = 0.4f;
   /** Default maximum load factor. */
   private static final float DEFAULT_MAX_LOAD_FACTOR = 0.8f;

   /** Minimum load factor. */
   private final float minLoadFactor;
   /** Maximum load factor. */
   private final float maxLoadFactor;

   /**
    * Opens an existing linear hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @return an existing linear hash index
    */
   public static LinearHashIndex openIndex(final BufferManager bufferManager, final String fileName) {
      final PageID headID = bufferManager.getDiskManager().getFileEntry(fileName);
      final Page<HashDirectoryHeader> header = bufferManager.pinPage(headID);
      final int numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      final int numEntries = HashDirectoryHeader.getSize(header);
      final SearchKeyType type = HashDirectoryHeader.getKeyType(header);
      final float minLoadFactor = HashDirectoryHeader.getMinLoadFactor(header);
      final float maxLoadFactor = HashDirectoryHeader.getMaxLoadFactor(header);
      bufferManager.unpinPage(header, UnpinMode.CLEAN);
      return new LinearHashIndex(bufferManager, Optional.of(fileName), headID, numBuckets, numEntries, type,
            minLoadFactor, maxLoadFactor);
   }

   /**
    * Creates a persistent linear hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param type
    *           the type of the search keys
    * @return an empty persistent linear hash index
    */
   public static LinearHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final SearchKeyType type) {
      final Page<HashDirectoryHeader> header = HashDirectoryHeader.newPage(bufferManager, 1,
                  DEFAULT_MIN_LOAD_FACTOR, DEFAULT_MAX_LOAD_FACTOR, type);
            final PageID headID = header.getPageID();
            bufferManager.unpinPage(header, UnpinMode.DIRTY);
      bufferManager.getDiskManager().addFileEntry(fileName, headID);
      return new LinearHashIndex(bufferManager, Optional.of(fileName), headID, 1, 0, type,
            DEFAULT_MIN_LOAD_FACTOR, DEFAULT_MAX_LOAD_FACTOR);
   }

   /**
    * Creates a temporary linear hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param type
    *           the type of the search keys
    * @return an empty temporary linear hash index
    */
   public static LinearHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final SearchKeyType type) {
      return createTemporaryIndex(bufferManager, type, DEFAULT_MIN_LOAD_FACTOR, DEFAULT_MAX_LOAD_FACTOR);
   }

   /**
    * Creates a temporary linear hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param type
    *           the type of the search keys
    * @param minLoadFactor
    *           minimum load factor
    * @param maxLoadFactor
    *           maximum load factor
    * @return an empty temporary linear hash index
    */
   public static LinearHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final SearchKeyType type, final float minLoadFactor, final float maxLoadFactor) {
      final Page<HashDirectoryHeader> header = HashDirectoryHeader.newPage(bufferManager, 1,
            minLoadFactor, maxLoadFactor, type);
      final PageID headID = header.getPageID();
      bufferManager.unpinPage(header, UnpinMode.DIRTY);
      return new LinearHashIndex(bufferManager, Optional.empty(), headID, 1, 0, type,
            minLoadFactor, maxLoadFactor);
   }

   /**
    * Constructs a linear hash index given an initialized header page id.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param headID
    *           page id of the header page for the index
    * @param numBuckets
    *           current number of buckets
    * @param numEntries
    *           number of entries currently stored in this index
    * @param type
    *           the type of the search keys
    * @param minLoadFactor
    *           minimum load factor
    * @param maxLoadFactor
    *           maximum load factor
    */
   private LinearHashIndex(final BufferManager bufferManager, final Optional<String> fileName,
         final PageID headID, final int numBuckets, final int numEntries, final SearchKeyType type,
         final float minLoadFactor, final float maxLoadFactor) {
      super(bufferManager, fileName, headID, numBuckets, numEntries, type);
      this.minLoadFactor = minLoadFactor;
      this.maxLoadFactor = maxLoadFactor;
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(rid);

      if (!rid.getPageID().isValid()) {
         throw new IllegalArgumentException("The record ID's page ID must be valid.");
      }

      // insert the entry into the bucket
      this.insert(this.getBucketNo(key), new IndexEntry(key, rid, this.getKeyType()));
      this.updateNumEntries(1);

      // check bucket split criterion
      if (this.currentLoadFactor() > this.maxLoadFactor) {
         this.splitBucket();
      }
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {
      final IndexEntry entry = new IndexEntry(Objects.requireNonNull(key), Objects.requireNonNull(rid),
            this.getKeyType());

      if (this.remove(this.getBucketNo(key), entry)) {
         this.updateNumEntries(-1);
         if (this.getNumBuckets() > MIN_NUM_BUCKETS && this.currentLoadFactor() < this.minLoadFactor) {
            this.mergeBucket();
         }
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
   private boolean remove(final int bucketNo, final IndexEntry entry) {
      final PageID firstPageID = this.getBucketID(bucketNo);
      if (!firstPageID.isValid()) {
         return false;
      }
      final BufferManager bufferManager = this.getBufferManager();
      final Page<BucketPage> primaryPage = bufferManager.pinPage(firstPageID);

      // iterate over bucket chain
      PageID deleteFromID = firstPageID;
      while (deleteFromID.isValid()) {
         final Page<BucketPage> deleteFrom = bufferManager.pinPage(deleteFromID);
         // iterate over every entry on the page
         for (int i = 0; i < BucketPage.getNumEntries(deleteFrom); ++i) {
            final IndexEntry e = BucketPage.readDataEntryAt(deleteFrom, i, entry.getKeyType());
            if (!entry.equals(e)) {
               continue;
            }
            // we have found our entry to remove
            /*
             * TODO is this check necessary? in the case we delete from the first page the primaryPage and
             * page are the same, so it would just move on the same page
             */
            if (deleteFromID.equals(firstPageID)) {
               // we are still on the primary page of the bucket
               BucketPage.removeDataEntryAt(deleteFrom, i, entry.getKeyType());
            } else {
               // we are on an overflow page, need to delete the entry there and get a new entry
               // from the primary page so our invariant that overflow pages are always full still holds
               BucketPage.moveLastEntryTo(primaryPage, deleteFrom, i, entry.getKeyType());
            }
            // the overflow page is now full again
            bufferManager.unpinPage(deleteFrom, UnpinMode.DIRTY);
            // check if primary page is now empty and chain needs to be shrunk
            if (BucketPage.getNumEntries(primaryPage) == 0) {
               final PageID nextID = BucketPage.getNextPagePointer(primaryPage);
               bufferManager.freePage(primaryPage);
               this.setDirectoryEntry(bucketNo, nextID);
            } else {
               // the primary page has still entries left
               bufferManager.unpinPage(primaryPage, UnpinMode.DIRTY);
            }
            return true;
         }
         // move along the chain
         deleteFromID = BucketPage.getNextPagePointer(deleteFrom);
         bufferManager.unpinPage(deleteFrom, UnpinMode.CLEAN);
      }

      bufferManager.unpinPage(primaryPage, UnpinMode.CLEAN);
      return false;
   }

   /**
    * Inserts the given data entry into the given bucket.
    *
    * @param bucketNo
    *           number of the bucket to insert the entry into
    * @param entry
    *           entry to insert
    */
   private void insert(final int bucketNo, final IndexEntry entry) {
      final BufferManager bufferManager = this.getBufferManager();
      final PageID bucketID = this.getBucketID(bucketNo);
      Page<BucketPage> page = null;
      if (!bucketID.isValid()) {
         // we have to create the first bucket page for the bucketNo
         page = BucketPage.newPage(bufferManager);
         this.setDirectoryEntry(bucketNo, page.getPageID());
      } else {
         /*
          * The invariant, that the overflow pages are always kept full, means that we do not need to
          * recursively insert into successive pages. Once we have a full primary page, we can just designate
          * it as the first overflow page in the overflow chain and fill the newly created primary page.
          */
         page = bufferManager.pinPage(bucketID);
         if (!BucketPage.hasSpaceLeft(page, this.getEntrySize())) {
            final PageID overflowID = page.getPageID();
            bufferManager.unpinPage(page, UnpinMode.CLEAN);
            page = BucketPage.newPage(bufferManager);
            BucketPage.setOverflowPointer(page, overflowID);
            this.setDirectoryEntry(bucketNo, page.getPageID());
         }
      }
      BucketPage.appendDataEntry(page, entry, this.getEntrySize());
      bufferManager.unpinPage(page, UnpinMode.DIRTY);
   }

   /**
    * Merges the last bucket with its sibling bucket.
    */
   private void mergeBucket() {
      final int fromBucketNo = this.getNumBuckets() - 1;
      // mask away the highest set bit
      final int toBucketNo = fromBucketNo & (Integer.highestOneBit(fromBucketNo) - 1);

      final PageID fromBucketID = this.getBucketID(fromBucketNo);
      if (!fromBucketID.isValid()) {
         // last bucket did not have entries anyway, so we are done here
         return;
      }

      final PageID toBucketStartID = this.getBucketID(toBucketNo);

      if (!toBucketStartID.isValid()) {
         // our sibling is not there, so we can just move the whole bucket from the last position
         this.setDirectoryEntry(toBucketNo, fromBucketID);
         return;
      }

      final BufferManager bufferManager = this.getBufferManager();
      final Page<BucketPage> fromBucket = bufferManager.pinPage(fromBucketID);
      final PageID nextFromBucketID = BucketPage.getNextPagePointer(fromBucket);

      if (nextFromBucketID.isValid()) {
         // move whole chain of bucket that gets deleted
         Page<BucketPage> toChainPage = bufferManager.pinPage(toBucketStartID);
         PageID next = BucketPage.getNextPagePointer(toChainPage);

         while (next.isValid()) {
            bufferManager.unpinPage(toChainPage, UnpinMode.CLEAN);
            toChainPage = bufferManager.pinPage(next);
            next = BucketPage.getNextPagePointer(toChainPage);
         }

         // found the last page in the chain
         BucketPage.setOverflowPointer(toChainPage, nextFromBucketID);
         bufferManager.unpinPage(toChainPage, UnpinMode.DIRTY);
      }

      final Page<BucketPage> toBucket = bufferManager.pinPage(toBucketStartID);

      if (BucketPage.merge(fromBucket, toBucket, this.getEntrySize())) {
         this.setDirectoryEntry(toBucketNo, fromBucketID);
         bufferManager.unpinPage(fromBucket, UnpinMode.DIRTY);
      } else {
         bufferManager.freePage(fromBucket);
      }

      bufferManager.unpinPage(toBucket, UnpinMode.DIRTY);

      // look for the place on the following directory pages
      PageID prevID = null;
      int offset = fromBucketNo;
      Page<HashDirectoryPage> dirPage = bufferManager.pinPage(this.getHeadID());
      int onPage = this.capacity(dirPage);
      while (offset >= onPage) {
         prevID = dirPage.getPageID();
         final PageID nextID = HashDirectoryPage.getNextPagePointer(dirPage);
         offset -= onPage;
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
         dirPage = bufferManager.pinPage(nextID);
         onPage = this.capacity(dirPage);
      }

      if (offset == 0 && prevID != null) {
         // the last directory page only contains one entry, remove it
         bufferManager.freePage(dirPage);
         final Page<HashDirectoryPage> prevPage = bufferManager.pinPage(prevID);
         HashDirectoryPage.setNextPagePointer(prevPage, PageID.INVALID);
         bufferManager.unpinPage(prevPage, UnpinMode.DIRTY);
      } else {
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      this.setNumBuckets(this.getNumBuckets() - 1);
   }

   /**
    * Splits the bucket at the next pointer.
    */
   private void splitBucket() {
      final int dirSize = this.getNumBuckets();
      final int oldBucketNo = dirSize ^ Integer.highestOneBit(dirSize);
      final int newBucketNo = dirSize;
      this.setNumBuckets(dirSize + 1);

      final PageID oldHeadID = this.getBucketID(oldBucketNo);

      if (!oldHeadID.isValid()) {
         this.setDirectoryEntry(newBucketNo, PageID.INVALID);
         return;
      }

      final int entrySize = this.getEntrySize();
      final int capacity = BucketPage.maxNumEntries(entrySize);

      final BufferManager bufferManager = this.getBufferManager();
      Page<BucketPage> oldHead = bufferManager.pinPage(oldHeadID);
      Page<BucketPage> oldRead = oldHead;
      Page<BucketPage> newBucket = null;
      int spaceInNew = capacity;

      /*
       * Invariants of this algorithm: - `oldHead` and `oldRead` are always pinned BucketPages - `oldHead` is
       * never later in the chain than `oldRead` - `oldHead` is the first page in the chain and the only one
       * that does not have to be full
       */
      PageID nextReadID;
      // loop over all pages of the old bucket and redistribute the entries
      for (;;) {
         nextReadID = BucketPage.getNextPagePointer(oldRead);
         int readPos = 0;

         // iterate over the entries inside the current bucket page
         for (int remaining = BucketPage.getNumEntries(oldRead); --remaining >= 0;) {
            final IndexEntry entry = BucketPage.readDataEntryAt(oldRead, readPos, this.getKeyType());
            final int destBucketNo = this.getBucketNo(entry.getSearchKey());
            if (destBucketNo == oldBucketNo) {
               // entry can stay, skip it
               readPos++;
               continue;
            }

            // found an entry to move
            if (oldRead == oldHead) {
               // we are on the first page, just delete the entry
               BucketPage.removeDataEntryAt(oldRead, readPos, this.getKeyType());
            } else {
               // refill the current page by stealing an entry from the first one
               if (BucketPage.moveLastEntryTo(oldHead, oldRead, readPos, this.getKeyType())) {
                  // the first page has become empty, free it
                  final PageID nextID = BucketPage.getNextPagePointer(oldHead);
                  bufferManager.freePage(oldHead);

                  // move the first page one step ahead
                  if (nextID.equals(oldRead.getPageID())) {
                     oldHead = oldRead;
                  } else {
                     oldHead = bufferManager.pinPage(nextID);
                  }
               }
               // already saw the entry at the readpos before
               readPos++;
            }

            // make sure that we have a page in the new bucket with capacity for the entry
            if (newBucket == null) {
               newBucket = BucketPage.newPage(bufferManager);
            } else if (spaceInNew == 0) {
               final PageID nextID = newBucket.getPageID();
               bufferManager.unpinPage(newBucket, UnpinMode.DIRTY);
               newBucket = BucketPage.newPage(bufferManager);
               BucketPage.setOverflowPointer(newBucket, nextID);
               spaceInNew = capacity;
            }

            // insert the entry
            BucketPage.appendDataEntry(newBucket, entry, this.getEntrySize());
            spaceInNew--;
         }

         if (!nextReadID.isValid()) {
            // no more pages to read, we are done
            break;
         }

         if (oldRead == oldHead) {
            oldRead = bufferManager.pinPage(nextReadID);
            if (BucketPage.getNumEntries(oldHead) == 0) {
               // first page is empty, free it and skip to the next one
               bufferManager.freePage(oldHead);
               oldHead = oldRead;
            }
         } else {
            // oldRead is completely filled
            bufferManager.unpinPage(oldRead, UnpinMode.DIRTY);
            oldRead = bufferManager.pinPage(nextReadID);
         }
      }

      if (BucketPage.getNumEntries(oldHead) == 0) {
         // the old bucket is empty, so oldRead and oldHead are the same
         bufferManager.freePage(oldHead);
         this.setDirectoryEntry(oldBucketNo, PageID.INVALID);
      } else {
         if (!oldHeadID.equals(oldHead.getPageID())) {
            this.setDirectoryEntry(oldBucketNo, oldHead.getPageID());
         }
         if (oldRead != oldHead) {
            bufferManager.unpinPage(oldRead, UnpinMode.DIRTY);
         }
         bufferManager.unpinPage(oldHead, UnpinMode.DIRTY);
      }

      if (newBucket == null) {
         this.setDirectoryEntry(newBucketNo, PageID.INVALID);
      } else {
         this.setDirectoryEntry(newBucketNo, newBucket.getPageID());
         bufferManager.unpinPage(newBucket, UnpinMode.DIRTY);
      }
   }

   /**
    * Gets the current load factor of the index.
    *
    * @return the current load factor of the index
    */
   private float currentLoadFactor() {
      return 1f * this.getNumEntries() / (this.getNumBuckets() * BucketPage.maxNumEntries(this.getEntrySize()));
   }

   /**
    * Sets the bucket id for the hash with the given bucket id.
    *
    * @param hash
    *           the hash value
    * @param bucketID
    *           the replacement bucket id
    * @throws IllegalStateException
    *            if the directory is not big enough to hold a bucket id for the given hash value
    */
   private void setDirectoryEntry(final int hash, final PageID bucketID) {
      final int numBuckets = this.getNumBuckets();
      if (hash < 0 || hash > numBuckets) {
         throw new IllegalArgumentException(
               "Bucket number " + hash + " out of range: " + "[0," + numBuckets + "]");
      }

      // look for the place on the following directory pages
      final BufferManager bufferManager = this.getBufferManager();
      PageID dirPageID = this.getHeadID();
      int offset = hash;
      Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirPageID);
      int onPage = this.capacity(dirPage);
      while (offset >= onPage) {
         dirPageID = HashDirectoryPage.getNextPagePointer(dirPage);
         offset -= onPage;
         if (dirPageID.isValid()) {
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            dirPage = bufferManager.pinPage(dirPageID);
         } else {
            final Page<HashDirectoryPage> newPage = HashDirectoryPage.newPage(bufferManager);
            HashDirectoryPage.setNextPagePointer(dirPage, newPage.getPageID());
            bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
            dirPage = newPage;
         }
         onPage = this.capacity(dirPage);
      }

      HashDirectoryPage.writePageID(dirPage, offset, bucketID);
      bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
   }

   @Override
   public String prettyPrint() {
      // append filename
      final String name = this.getFileName().orElse("(temp)");
      final StringBuilder sb = new StringBuilder(name).append("\n");
      for (int i = name.length(); --i >= 0;) {
         sb.append('-');
      }
      sb.append("\n");

      // append directory listing
      // show which buckets are allocated and how many entries each bucket contains

      int currBucketNum = 0;

      int totalEntries = 0;

      // append buckets stored on directory pages
      final BufferManager bufferManager = this.getBufferManager();
      final int dirSize = this.getNumBuckets();
      PageID dirID = this.getHeadID();
      int numDirPages = 0;
      while (dirID.isValid()) {
         numDirPages++;
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         // we do not want to read more slots than we have left or that are on the page
         final int dirBuckets = Math.min(dirSize - currBucketNum, this.capacity(dirPage));
         for (int i = 0; i < dirBuckets; ++i) {
            final String hash = Integer.toString(currBucketNum, 2);
            final int level = 31 - Integer.numberOfLeadingZeros(dirSize);
            for (int j = 0; j < level - hash.length() + 1; ++j) {
               sb.append('0');
            }
            sb.append(hash + " : ");
            final PageID bucketID = HashDirectoryPage.readPageID(dirPage, i);
            if (bucketID.isValid()) {
               final Page<BucketPage> firstPage = bufferManager.pinPage(bucketID);
               int bucketEntries = BucketPage.getNumEntries(firstPage);
               PageID nextID = BucketPage.getNextPagePointer(firstPage);
               while (nextID.isValid()) {
                  final Page<BucketPage> bucketPage = bufferManager.pinPage(nextID);
                  bucketEntries += BucketPage.getNumEntries(bucketPage);
                  nextID = BucketPage.getNextPagePointer(bucketPage);
                  bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
               }
               totalEntries += bucketEntries;
               bufferManager.unpinPage(firstPage, UnpinMode.CLEAN);
               sb.append(bucketEntries);
            } else {
               sb.append("null");
            }
            sb.append("\n");
            currBucketNum++;
         }

         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      for (int j = 0; j < this.getFileName().orElse("(temp)").length(); ++j) {
         sb.append('-');
      }
      sb.append("\n");
      sb.append("Total # entries (counted): " + this.getNumEntries() + " (" + totalEntries + ")" + "\n");
      sb.append("Total # buckets (counted): " + this.getNumBuckets() + " (" + currBucketNum + ")" + "\n");
      sb.append("# directory pages: " + numDirPages + "\n");
      return sb.toString();
   }

   @Override
   public void checkInvariants() {
      PageID dirID = this.getHeadID();
      int seen = 0;
      int numEntries = 0;
      final int entrySize = this.getEntrySize();
      final int bucketCap = BucketPage.maxNumEntries(entrySize);
      final BufferManager bufferManager = this.getBufferManager();
      final int numBuckets = this.getNumBuckets();
      do {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int onPage = Math.min(this.capacity(dirPage), numBuckets - seen);
         if (onPage == 0) {
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            throw new AssertionError("Directory too long.");
         }

         for (int i = 0; i < onPage; i++) {
            final int hash = seen + i;
            final PageID bucketID = HashDirectoryPage.readPageID(dirPage, i);
            PageID currID = bucketID;
            while (currID.isValid()) {
               final Page<BucketPage> bucketPage = bufferManager.pinPage(currID);
               final int entries = BucketPage.getNumEntries(bucketPage);
               if (entries == 0 || currID != bucketID && entries != bucketCap) {
                  bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
                  bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
                  throw new AssertionError("Underfull bucket page in bucket " + hash + ": " + currID);
               }

               for (int j = 0; j < entries; j++) {
                  final SearchKey key = BucketPage.readSearchKey(bucketPage, j, this.getKeyType());
                  if (this.getBucketNo(key) != hash) {
                     bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
                     bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
                     throw new AssertionError("Wrong entry in bucket " + hash + ": " + key);
                  }
               }
               currID = BucketPage.getNextPagePointer(bucketPage);
               bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
               numEntries += entries;
            }
         }
         seen += onPage;
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      } while (dirID.isValid());

      if (seen != numBuckets) {
         throw new AssertionError("Directory is too short: Expected " + numBuckets + ", found " + seen);
      }

      if (numEntries != this.getNumEntries()) {
         throw new AssertionError(
               "Wrong number of entries: Expected " + this.getNumEntries() + ", found " + numEntries);
      }
   }

   @Override
   void writeAdditionalMetadata(final Page<HashDirectoryHeader> header) {
      HashDirectoryHeader.setMinLoadFactor(header, this.minLoadFactor);
      HashDirectoryHeader.setMaxLoadFactor(header, this.maxLoadFactor);
   }

   @Override
   int getBucketNo(final SearchKey key) {
      final int hashCode = key.hashCode();
      final int dirSize = this.getNumBuckets();
      final int level = 31 - Integer.numberOfLeadingZeros(dirSize);
      int hash = nBits(hashCode, level);
      final int next = dirSize ^ (1 << level);
      if (hash < next) {
         // rehash if bucket already split
         hash = nBits(hashCode, level + 1);
      }
      return hash;
   }

   @Override
   int capacity(final Page<? extends HashDirectoryPage> dirPage) {
      return HashDirectoryPage.getWritableSize(dirPage) / PageID.BYTES;
   }

   @Override
   boolean isValidPrimaryBucket(final int hash, final Page<HashDirectoryPage> dirPage, final int offset) {
      return HashDirectoryPage.readPageID(dirPage, offset).isValid();
   }
}

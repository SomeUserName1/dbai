/*
 * @(#)ExtendibleHashIndex.java   1.0   Dec 6, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */

package minibase.access.hash;

import java.util.Arrays;
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
 * This is a simple implementation of a extendible HashIndex. It starts with one bucket and grows as needed.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
public final class ExtendibleHashIndex extends HashIndex {
   /** Size of a directory entry. */
   private static final int DIRECTORY_ENTRY_SIZE = PageID.BYTES + Byte.BYTES;

   /** Global depth of the hash index. */
   private int depth;

   /**
    * Opens an existing extendible hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @return an existing linear hash index
    */
   public static ExtendibleHashIndex openIndex(final BufferManager bufferManager, final String fileName) {
      final PageID headID = bufferManager.getDiskManager().getFileEntry(fileName);
      final Page<HashDirectoryHeader> header = bufferManager.pinPage(headID);
      final int numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      final int numEntries = HashDirectoryHeader.getSize(header);
      final SearchKeyType type = HashDirectoryHeader.getKeyType(header);
      bufferManager.unpinPage(header, UnpinMode.CLEAN);
      return new ExtendibleHashIndex(bufferManager, Optional.of(fileName), headID, numBuckets, numEntries, type);
   }

   /**
    * Creates a persistent extendible hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param type
    *           the type of the search keys
    * @return an empty persistent linear hash index
    */
   public static ExtendibleHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final SearchKeyType type) {
      return createIndex(bufferManager, Optional.of(fileName), type);
   }

   /**
    * Creates a temporary extendible hash index. Note: although it is a temporary index, you have to delete
    * the index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param type
    *           the type of the search keys
    * @return an empty temporary linear hash index
    */
   public static ExtendibleHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final SearchKeyType type) {
      return createIndex(bufferManager, Optional.empty(), type);
   }

   /**
    * Creates a extendible hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param type
    *           the type of the search keys
    * @return an empty persistent linear hash index
    */
   private static ExtendibleHashIndex createIndex(final BufferManager bufferManager,
         final Optional<String> fileName, final SearchKeyType type) {

      final Page<BucketPage> bucket = BucketPage.newPage(bufferManager);
      final PageID bucketID = bucket.getPageID();
      bufferManager.unpinPage(bucket, UnpinMode.DIRTY);

      final Page<HashDirectoryHeader> header =
            HashDirectoryHeader.newPage(bufferManager, 1, Float.NaN, Float.NaN, type);
      final PageID headID = header.getPageID();
      HashDirectoryPage.writePageID(header, 0, bucketID);
      HashDirectoryPage.writeDepth(header, 0, (byte) 0);
      bufferManager.unpinPage(header, UnpinMode.DIRTY);

      fileName.ifPresent(x -> bufferManager.getDiskManager().addFileEntry(x, headID));
      return new ExtendibleHashIndex(bufferManager, fileName, headID, 1, 0, type);
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
    * @param numBuckets
    *           current number of buckets
    * @param numEntries
    *           number of entries currently stored in the index
    * @param type
    *           the type of the search keys
    */
   private ExtendibleHashIndex(final BufferManager bufferManager, final Optional<String> fileName,
         final PageID headID, final int numBuckets, final int numEntries, final SearchKeyType type) {
      super(bufferManager, fileName, headID, numBuckets, numEntries, type);
      this.depth = 31 - Integer.numberOfLeadingZeros(numBuckets);
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(rid);
      if (!rid.getPageID().isValid()) {
         throw new IllegalArgumentException("The record ID's page ID must be valid.");
      }
      final BufferManager bufferManager = this.getBufferManager();
      final int entrySize = this.getEntrySize();
      final IndexEntry entry = new IndexEntry(key, rid, this.getKeyType());

      // search dataPage for key and insert
      final int hash = nBits(key.hashCode(), this.depth);
      final PageID bucketID;
      final int localDepth;

      int offset = hash;
      for (PageID dirID = this.getHeadID();;) {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int numEntries = this.capacity(dirPage);
         if (offset < numEntries) {
            bucketID = HashDirectoryPage.readPageID(dirPage, offset);
            localDepth = HashDirectoryPage.readDepth(dirPage, offset);
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            break;
         }
         offset -= numEntries;
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      final Page<BucketPage> bucket = bufferManager.pinPage(bucketID);
      if (BucketPage.hasSpaceLeft(bucket, entrySize)) {
         BucketPage.appendDataEntry(bucket, entry, entrySize);
         bufferManager.unpinPage(bucket, UnpinMode.DIRTY);
      } else {
         // move the contents of the first page to the next one so that the it stays partially filled
         // and the entries in the directory stay valid
         final Page<BucketPage> overflow = bufferManager.copyPage(bucket);
         final PageID overflowID = overflow.getPageID();
         bufferManager.unpinPage(overflow, UnpinMode.DIRTY);

         BucketPage.setOverflowPointer(bucket, overflowID);
         BucketPage.setNumEntries(bucket, 0);
         BucketPage.appendDataEntry(bucket, entry, entrySize);
         bufferManager.unpinPage(bucket, UnpinMode.DIRTY);

         // split the bucket if possible
         this.splitBucket(nBits(hash, localDepth), bucketID, localDepth);
      }

      this.updateNumEntries(1);
   }

   /**
    * Splits the bucket with the given hash, bucket page ID and local depth.
    * @param hash hash value of the bucket to split
    * @param bucketID page ID of the bucket's first page
    * @param depth local depth of the bucket
    */
   private void splitBucket(final int hash, final PageID bucketID, final int depth) {
      if (depth == Integer.SIZE) {
         // do not split the directory if we already use all bits of the hash values
         return;
      }

      // output pages
      final BufferManager bufferManager = this.getBufferManager();
      final int entrySize = this.getEntrySize();
      Page<BucketPage> leftPage = bufferManager.pinPage(bucketID);
      int leftPos = 0;
      Page<BucketPage> rightPage = BucketPage.newPage(bufferManager);
      int rightPos = 0;

      final PageID rightBucketID = rightPage.getPageID();
      final int bucketPageCap = BucketPage.maxNumEntries(entrySize);

      // partition the entries into the two new buckets, reusing pages
      PageID nextID = bucketID;
      PageID nextFree = BucketPage.getNextPagePointer(leftPage);
      do {
         final Page<BucketPage> readPage = bufferManager.pinPage(nextID);
         final int onPage = BucketPage.getNumEntries(readPage);
         nextID = BucketPage.getNextPagePointer(readPage);

         for (int readPos = 0; readPos < onPage; readPos++) {
            final SearchKey key = BucketPage.readSearchKey(readPage, readPos, this.getKeyType());
            if (hash == nBits(key.hashCode(), depth + 1)) {
               // entry goes to the left bucket
               if (readPage == leftPage && readPos == leftPos) {
                  // skip entries at the start that are in the correct bucket
                  leftPos++;
                  continue;
               }

               if (leftPos == bucketPageCap) {
                  // left bucket page is full
                  BucketPage.setNumEntries(leftPage, bucketPageCap);
                  BucketPage.setOverflowPointer(leftPage, nextFree);
                  bufferManager.unpinPage(leftPage, UnpinMode.DIRTY);
                  leftPage = bufferManager.pinPage(nextFree);
                  nextFree = BucketPage.getNextPagePointer(leftPage);
                  leftPos = 0;
               }

               BucketPage.moveEntry(readPage, readPos, leftPage, leftPos, this.getKeyType());
               leftPos++;
            } else {
               // entry goes to the right bucket
               if (rightPos == bucketPageCap) {
                  // right bucket page is full
                  BucketPage.setNumEntries(rightPage, bucketPageCap);
                  BucketPage.setOverflowPointer(rightPage, nextFree);
                  bufferManager.unpinPage(rightPage, UnpinMode.DIRTY);
                  rightPage = bufferManager.pinPage(nextFree);
                  nextFree = BucketPage.getNextPagePointer(rightPage);
                  rightPos = 0;
               }

               BucketPage.moveEntry(readPage, readPos, rightPage, rightPos, this.getKeyType());
               rightPos++;
            }
         }

         bufferManager.unpinPage(readPage, UnpinMode.CLEAN);
      } while (nextID.isValid());

      // finish buckets and recover first-page invariant
      BucketPage.setNumEntries(leftPage, leftPos);
      this.finishBucket(bucketID, leftPage, leftPos);
      bufferManager.unpinPage(leftPage, UnpinMode.DIRTY);

      BucketPage.setNumEntries(rightPage, rightPos);
      this.finishBucket(rightBucketID, rightPage, rightPos);
      bufferManager.unpinPage(rightPage, UnpinMode.DIRTY);

      // delete remaining pages
      while (nextFree.isValid()) {
         final Page<BucketPage> free = bufferManager.pinPage(nextFree);
         nextFree = BucketPage.getNextPagePointer(free);
         bufferManager.freePage(free);
      }

      // update the relevant entries in the directory and split it if it is too small
      if (depth < this.depth) {
         this.updateDirectory(hash, depth, depth + 1, bucketID, rightBucketID);
      } else {
         this.splitDirectory(hash, rightBucketID);
      }
   }

   /**
    * Finishes a bucket resulting from a split, making sure that only the first page has free space.
    * @param bucketID page ID of the bucket's first page
    * @param lastPage last page of the bucket
    * @param inPage number of entries in the last page
    */
   private void finishBucket(final PageID bucketID, final Page<BucketPage> lastPage, final int inPage) {
      final int entrySize = this.getEntrySize();
      final int bucketPageCap = BucketPage.maxNumEntries(entrySize);
      final int rightGap = bucketPageCap - inPage;
      if (rightGap > 0 && !bucketID.equals(lastPage.getPageID())) {
         // last page is not full
         final int off = inPage * entrySize;
         final BufferManager bufferManager = this.getBufferManager();
         final Page<BucketPage> firstPage = bufferManager.pinPage(bucketID);
         System.arraycopy(firstPage.getData(), off, lastPage.getData(), off, rightGap * entrySize);
         BucketPage.setNumEntries(firstPage, inPage);
         BucketPage.setNumEntries(lastPage, bucketPageCap);
         bufferManager.unpinPage(firstPage, UnpinMode.DIRTY);
      }
      BucketPage.setOverflowPointer(lastPage, PageID.INVALID);
   }

   /**
    * Doubles the size of the directory after a bucket was split.
    *
    * @param splitHash
    *           hash value of the bucket that was split
    * @param newBucketID
    *           page ID of the new bucket that resulted from the split
    */
   private void splitDirectory(final int splitHash, final PageID newBucketID) {
      int readPos = 0;
      final BufferManager bufferManager = this.getBufferManager();
      final int numBuckets = this.getNumBuckets();
      Page<HashDirectoryPage> readPage = bufferManager.pinPage(this.getHeadID());
      int onReadPage = this.capacity(readPage);
      UnpinMode readPageMode = UnpinMode.CLEAN;

      int writePos = numBuckets;
      Page<HashDirectoryPage> writePage = bufferManager.pinPage(this.getHeadID());
      int onWritePage = this.capacity(writePage);
      PageID nextID = HashDirectoryPage.getNextPagePointer(writePage);
      while (nextID.isValid()) {
         writePos -= onWritePage;
         bufferManager.unpinPage(writePage, UnpinMode.CLEAN);
         writePage = bufferManager.pinPage(nextID);
         onWritePage = this.capacity(writePage);
         nextID = HashDirectoryPage.getNextPagePointer(writePage);
      }

      for (int hash = 0; hash < numBuckets; ++hash) {
         if (readPos >= onReadPage) {
            final PageID next = HashDirectoryPage.getNextPagePointer(readPage);
            bufferManager.unpinPage(readPage, readPageMode);
            readPos = 0;
            readPage = bufferManager.pinPage(next);
            onReadPage = this.capacity(readPage);
            readPageMode = UnpinMode.CLEAN;
         }

         if (writePos >= onWritePage) {
            final Page<HashDirectoryPage> next = HashDirectoryPage.newPage(bufferManager);
            HashDirectoryPage.setNextPagePointer(writePage, next.getPageID());
            bufferManager.unpinPage(writePage, UnpinMode.DIRTY);
            writePos = 0;
            writePage = next;
            onWritePage = this.capacity(writePage);
         }

         final PageID bucketID = HashDirectoryPage.readPageID(readPage, readPos);
         final byte depth = HashDirectoryPage.readDepth(readPage, readPos);

         if (hash == splitHash) {
            HashDirectoryPage.writeDepth(readPage, readPos, (byte) (depth + 1));
            readPageMode = UnpinMode.DIRTY;
            HashDirectoryPage.writePageID(writePage, writePos, newBucketID);
            HashDirectoryPage.writeDepth(writePage, writePos, (byte) (depth + 1));
         } else {
            HashDirectoryPage.writePageID(writePage, writePos, bucketID);
            HashDirectoryPage.writeDepth(writePage, writePos, depth);
         }
         ++readPos;
         ++writePos;
      }
      bufferManager.unpinPage(readPage, readPageMode);
      bufferManager.unpinPage(writePage, UnpinMode.DIRTY);

      this.setNumBuckets(2 * numBuckets);
      this.depth++;
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {
      final IndexEntry entry =
            new IndexEntry(Objects.requireNonNull(key), Objects.requireNonNull(rid), this.getKeyType());
      final BufferManager bufferManager = this.getBufferManager();
      final int hash = nBits(key.hashCode(), this.depth);
      final PageID firstPageID;
      final int depth;

      // find the bucket in the directory
      int offset = hash;
      for (PageID dirID = this.getHeadID();;) {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int numEntries = this.capacity(dirPage);
         if (offset < numEntries) {

            firstPageID = HashDirectoryPage.readPageID(dirPage, offset);
            depth = HashDirectoryPage.readDepth(dirPage, offset);
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            break;
         }
         offset -= numEntries;
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      // iterate over bucket chain
      Page<BucketPage> page = bufferManager.pinPage(firstPageID);
      final int onFirstPage = BucketPage.getNumEntries(page);
      int onPage = onFirstPage;
      int entryPos = -1;
      for (;;) {
         // iterate over every entry on the page
         for (int i = 0; i < onPage; i++) {
            final IndexEntry e = BucketPage.readDataEntryAt(page, i, entry.getKeyType());
            if (entry.equals(e)) {
               entryPos = i;
               break;
            }
         }
         if (entryPos >= 0) {
            break;
         }

         // move along the chain
         final PageID nextID = BucketPage.getNextPagePointer(page);
         bufferManager.unpinPage(page, UnpinMode.CLEAN);
         if (!nextID.isValid()) {
            // entry is not present
            return false;
         }
         page = bufferManager.pinPage(nextID);
         onPage = BucketPage.getNumEntries(page);
      }

      // delete the entry from the bucket page
      final Page<BucketPage> firstPage = bufferManager.pinPage(firstPageID);
      UnpinMode firstPageMode = UnpinMode.CLEAN;
      if (page.getPageID().equals(firstPageID)) {
         // we are still on the primary page of the bucket
         BucketPage.removeDataEntryAt(page, entryPos, this.getKeyType());
      } else {
         // we are on an overflow page, need to delete the entry there and get a new entry
         // from the primary page so our invariant that overflow pages are always full still holds
         BucketPage.moveLastEntryTo(firstPage, page, entryPos, this.getKeyType());
         firstPageMode = UnpinMode.DIRTY;
      }
      bufferManager.unpinPage(page, UnpinMode.DIRTY);

      // check if first page is now empty and chain needs to be shrunk
      final PageID firstNextID = BucketPage.getNextPagePointer(firstPage);
      if (onFirstPage == 1 && firstNextID.isValid()) {
         final Page<BucketPage> nextPage = bufferManager.pinPage(firstNextID);
         BucketPage.copyContents(nextPage, firstPage);
         bufferManager.freePage(nextPage);
         firstPageMode = UnpinMode.DIRTY;
      }

      final int inFirst = BucketPage.getNumEntries(firstPage);
      bufferManager.unpinPage(firstPage, firstPageMode);

      if (inFirst == 0) {
         // check if this empty bucket can be merged with its neighbors
         this.mergeBuckets(hash, firstPageID, depth);
      }

      this.updateNumEntries(-1);
      return true;
   }

   /**
    * Tries to merge the bucket with the given hash value with its neighbor until either
    * <ul>
    *    <li>the neighbor has a higher local depth,
    *    <li>the resulting bucket is non-empty,</li>
    *    <li>or there is only one bucket left.
    * </ul>
    * @param hash hash of the empty bucket to merge
    * @param bucketID page ID of the bucket page
    * @param oldDepth local depth of the bucket
    */
   private void mergeBuckets(final int hash, final PageID bucketID, final int oldDepth) {
      final BufferManager bufferManager = this.getBufferManager();
      PageID currBucketID = bucketID;
      for (int depth = oldDepth; depth > 0; depth--) {
         final int neighborHash = (hash & ((1 << depth) - 1)) ^ (1 << (depth - 1));
         final PageID neighborID;
         final int neighborDepth;

         // find the neighbor in the directory
         int neighborOffset = neighborHash;
         for (PageID dirID = this.getHeadID();;) {
            final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
            final int numEntries = this.capacity(dirPage);
            if (neighborOffset < numEntries) {
               neighborID = HashDirectoryPage.readPageID(dirPage, neighborOffset);
               neighborDepth = HashDirectoryPage.readDepth(dirPage, neighborOffset);
               bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               break;
            }
            neighborOffset -= numEntries;
            dirID = HashDirectoryPage.getNextPagePointer(dirPage);
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
         }

         if (depth != neighborDepth) {
            // neighbors cannot be merged
            return;
         }

         bufferManager.freePage(bufferManager.pinPage(currBucketID));
         currBucketID = neighborID;

         final int newDepth = depth - 1;
         this.updateDirectory(nBits(hash, newDepth), newDepth, newDepth, neighborID, neighborID);

         if (depth == this.depth) {
            // the bucket had maximum depth, try to shrink the directory
            this.shrinkDirectory();
         }

         // check if the merged bucket is also empty and the merging should continue
         final Page<BucketPage> bucket = bufferManager.pinPage(currBucketID);
         final int onFirst = BucketPage.getNumEntries(bucket);
         bufferManager.unpinPage(bucket, UnpinMode.CLEAN);
         if (onFirst > 0) {
            // bucket is non-empty
            return;
         }
      }
   }

   /**
    * Shrinks the hash directory if all buckets have a local depth that is lower that the global depth.
    *
    * @return {@code true} if the directory was shrunk, {@code false} otherwise
    */
   private boolean shrinkDirectory() {
      int hash = 0;
      int readPos = 0;
      final BufferManager bufferManager = this.getBufferManager();
      Page<HashDirectoryPage> dirPage = bufferManager.pinPage(this.getHeadID());
      int onPage = this.capacity(dirPage);
      int maxDepth = -1;
      for (int k = 0; k < this.depth; k++) {
         final int size = 1 << k;
         while (hash < size) {
            if (readPos >= onPage) {
               final PageID nextID = HashDirectoryPage.getNextPagePointer(dirPage);
               bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               dirPage = bufferManager.pinPage(nextID);
               onPage = this.capacity(dirPage);
               readPos = 0;
            }
            maxDepth = Math.max(maxDepth, HashDirectoryPage.readDepth(dirPage, readPos));
            if (maxDepth == this.depth) {
               // there is a bucket with the global depth, so nothing can be shrunk
               bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               return false;
            }
            readPos++;
            hash++;
         }

         if (maxDepth <= k) {
            // the directory can be shrunk
            this.depth = k;
            this.setNumBuckets(size);
            PageID delID = HashDirectoryPage.getNextPagePointer(dirPage);
            HashDirectoryPage.setNextPagePointer(dirPage, PageID.INVALID);
            bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
            while (delID.isValid()) {
               final Page<HashDirectoryPage> deleteMe = bufferManager.pinPage(delID);
               delID = HashDirectoryPage.getNextPagePointer(deleteMe);
               bufferManager.freePage(deleteMe);
            }
            return true;
         }
      }
      bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      return false;
   }

   /**
    * Updates the entries for the given hash value in the hash directory.
    * @param hash hash value to update the entries for
    * @param oldDepth old local depth of the hash value
    * @param newDepth new depth to write
    * @param leftID bucket page ID for those entries where the highest bit is {@code 0}
    * @param rightID bucket page ID for those entries where the highest bit is {@code 1}
    */
   private void updateDirectory(final int hash, final int oldDepth, final int newDepth,
         final PageID leftID, final PageID rightID) {
      final BufferManager bufferManager = this.getBufferManager();
      Page<HashDirectoryPage> dirPage = bufferManager.pinPage(this.getHeadID());
      int capacity = this.capacity(dirPage);
      UnpinMode dirPageMode = UnpinMode.CLEAN;

      int skipped = 0;
      final int k = 1 << (this.depth - oldDepth);
      for (int i = 0; i < k; i++) {
         final int next = hash + i * (1 << oldDepth);
         while (next - skipped >= capacity) {
            skipped += capacity;
            final PageID nextID = HashDirectoryPage.getNextPagePointer(dirPage);
            bufferManager.unpinPage(dirPage, dirPageMode);
            dirPage = bufferManager.pinPage(nextID);
            capacity = this.capacity(dirPage);
            dirPageMode = UnpinMode.CLEAN;
         }
         HashDirectoryPage.writePageID(dirPage, next - skipped, i % 2 == 0 ? leftID : rightID);
         HashDirectoryPage.writeDepth(dirPage, next - skipped, (byte) newDepth);
         dirPageMode = UnpinMode.DIRTY;
      }

      bufferManager.unpinPage(dirPage, dirPageMode);
   }

   @Override
   boolean isValidPrimaryBucket(final int hash, final Page<HashDirectoryPage> dirPage, final int pos) {
      final int depth = HashDirectoryPage.readDepth(dirPage, pos);
      return nBits(hash, depth) == nBits(hash, this.depth);
   }

   @Override
   public String prettyPrint() {
      // append filename
      final StringBuilder sb = new StringBuilder(this.getFileName().orElse("Temporary Index")).append("\n");
      for (int i = 0; i < this.getFileName().orElse("Temporary Index").length(); i++) {
         sb.append('-');
      }
      sb.append("\n");

      int totalEntries = 0;
      int currBucketNum = 0;
      int numBktAlloc = 0;
      int numDirPages = 0;
      // append directory listing
      // show which buckets are allocated and how many entries each bucket contains
      PageID pageID = this.getHeadID();
      final BufferManager bufferManager = this.getBufferManager();
      while (pageID.isValid()) {
         final Page<HashDirectoryPage> page = bufferManager.pinPage(pageID);
         final int numEntries = this.capacity(page);
         for (int i = 0; i < numEntries && currBucketNum < this.getNumBuckets(); i++) {
            final PageID bucketID = HashDirectoryPage.readPageID(page, i);
            final byte depth = HashDirectoryPage.readDepth(page, i);

            final String hash = Integer.toString(currBucketNum, 2);
            for (int j = 0; j < this.depth - hash.length(); ++j) {
               sb.append('0');
            }
            sb.append(hash + " (" + depth + "): ");
            if (currBucketNum % (1 << depth) == currBucketNum) {
               final Page<BucketPage> firstPage = bufferManager.pinPage(bucketID);
               int bucketEntries = BucketPage.getNumEntries(firstPage);
               PageID nextID = BucketPage.getNextPagePointer(firstPage);
               while (nextID.isValid()) {
                  final Page<BucketPage> bucketPage = bufferManager.pinPage(nextID);
                  bucketEntries += BucketPage.getNumEntries(bucketPage);
                  nextID = BucketPage.getNextPagePointer(bucketPage);
                  bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
               }
               bufferManager.unpinPage(firstPage, UnpinMode.CLEAN);
               totalEntries += bucketEntries;
               ++numBktAlloc;
               sb.append(bucketEntries);
            } else {
               sb.append("->" + (currBucketNum % (1 << depth)));
            }
            ++currBucketNum;
            sb.append("\n");
         }
         pageID = HashDirectoryPage.getNextPagePointer(page);
         bufferManager.unpinPage(page, UnpinMode.CLEAN);
         numDirPages++;
      }

      for (int j = 0; j < this.getFileName().orElse("Temporary Index").length(); ++j) {
         sb.append('-');
      }

      sb.append("\n");
      sb.append("Total # entries (counted): " + this.getNumEntries() + " (" + totalEntries + ")\n");
      sb.append("Total # buckets (counted, allocated): " + this.getNumBuckets()
            + " (" + currBucketNum + ", " + numBktAlloc + ")\n");
      sb.append("# directory pages: " + numDirPages + "\n");
      sb.append("Global Depth: " + this.depth + "\n");
      return sb.toString();
   }

   @Override
   public void checkInvariants() {
      final int[] directory = new int[this.getNumBuckets()];
      Arrays.fill(directory, -1);

      int entries = 0;
      PageID dirID = this.getHeadID();
      int seen = 0;
      final BufferManager bufferManager = this.getBufferManager();
      final int entrySize = this.getEntrySize();
      do {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int cap = this.capacity(dirPage);
         final int onPage = Math.min(cap, directory.length - seen);
         if (onPage == 0) {
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            throw new AssertionError("Directory too long.");
         }

         for (int i = 0; i < onPage; i++) {
            final int localDepth = HashDirectoryPage.readDepth(dirPage, i);
            if (localDepth > this.depth) {
               bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               throw new AssertionError("Local depth greater that global: " + localDepth + " > " + this.depth);
            }

            final PageID bucketID = HashDirectoryPage.readPageID(dirPage, i);
            final int hash = seen + i;
            if (nBits(hash, localDepth) == hash) {
               for (int j = hash; j < directory.length; j += 1 << localDepth) {
                  directory[j] = bucketID.getValue();
               }

               final Page<BucketPage> bucketPage = bufferManager.pinPage(bucketID);
               final int onBucketPage = BucketPage.getNumEntries(bucketPage);
               PageID overflowID = BucketPage.getNextPagePointer(bucketPage);
               bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);

               if (onBucketPage == 0 && overflowID.isValid()) {
                  bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
                  throw new AssertionError("Empty first page with overflow chain: " + hash);
               }

               int entriesSeen = onBucketPage;
               while (overflowID.isValid()) {
                  final Page<BucketPage> overflowPage = bufferManager.pinPage(overflowID);
                  final int onOverflowPage = BucketPage.getNumEntries(overflowPage);
                  entriesSeen += onOverflowPage;
                  overflowID = BucketPage.getNextPagePointer(overflowPage);
                  bufferManager.unpinPage(overflowPage, UnpinMode.CLEAN);

                  if (onOverflowPage != BucketPage.maxNumEntries(entrySize)) {
                     bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
                     throw new AssertionError("Overflow page is not filled: " + onOverflowPage);
                  }
               }
               entries += entriesSeen;
            } else if (directory[hash] == -1 || directory[hash] != bucketID.getValue()) {
               bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               throw new AssertionError("Wrong entry for bucket " + hash + ": " + bucketID);
            }
         }
         seen += onPage;
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      } while (dirID.isValid());

      if (entries != this.getNumEntries()) {
         throw new AssertionError("Wrong number of entries: Expected " + this.getNumEntries() + ", found " + entries);
      }
   }

   /**
    * Gets the depth of the Bucket for the hash with the given entry.
    *
    * @param hash
    *           bucket number
    * @return depth of the bucket
    */
   byte getDepth(final int hash) {
      if (hash >= this.getNumBuckets()) {
         throw new IllegalArgumentException();
      }
      // we start with the first bucket entries on the header page
      PageID pageID = this.getHeadID();

      int offset = hash;
      final BufferManager bufferManager = this.getBufferManager();
      while (pageID.isValid()) {
         final Page<HashDirectoryPage> page = bufferManager.pinPage(pageID);
         pageID = HashDirectoryPage.getNextPagePointer(page);
         final int num = this.capacity(page);
         if (offset < num) {
            final byte depth = HashDirectoryPage.readDepth(page, offset);
            bufferManager.unpinPage(page, UnpinMode.CLEAN);
            return depth;
         }
         bufferManager.unpinPage(page, UnpinMode.CLEAN);
         // we have not found the bucket on the current page,
         // so proceed with a smaller offset for the next page
         offset -= num;
      }

      // we have no bucket for the given hash
      throw new IllegalStateException();
   }

   @Override
   int getBucketNo(final SearchKey key) {
      return nBits(key.hashCode(), this.depth);
   }

   /**
    * Returns the current global depth of this index.
    *
    * @return current global depth
    */
   int getGlobalDepth() {
      return this.depth;
   }

   /**
    * Computes the number of directory entries that fit onto the given header page.
    *
    * @param dirPage directory header page
    * @return the given page's capacity
    */
   int capacity(final Page<? extends HashDirectoryPage> dirPage) {
      return HashDirectoryPage.getWritableSize(dirPage) / DIRECTORY_ENTRY_SIZE;
   }

   @Override
   void writeAdditionalMetadata(final Page<HashDirectoryHeader> header) {
   }
}

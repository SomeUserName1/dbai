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
 * This is a simple implementation of a static HashIndex.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
public final class StaticHashIndex extends HashIndex {

   /** Default number of buckets if none is given. */
   private static final int DEFAULT_SIZE = 1024;

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
      final Page<HashDirectoryHeader> header = bufferManager.pinPage(headID);
      final int numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      final int numEntries = HashDirectoryHeader.getSize(header);
      final SearchKeyType type = HashDirectoryHeader.getKeyType(header);
      bufferManager.unpinPage(header, UnpinMode.CLEAN);
      return new StaticHashIndex(bufferManager, Optional.of(fileName), headID, numBuckets, numEntries, type);
   }

   /**
    * Creates a persistent static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param type
    *           the type of the search keys
    * @param depth
    *           depth of the index
    * @return an empty persistent linear hash index
    */
   public static StaticHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final SearchKeyType type, final byte depth) {
      return createIndex(bufferManager, Optional.of(fileName), type, depth);
   }

   /**
    * Creates a persistent static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param type
    *           the type of the search keys
    * @return an empty persistent linear hash index
    */
   public static StaticHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final SearchKeyType type) {
      return createIndex(bufferManager, Optional.of(fileName), type, DEFAULT_SIZE);
   }

   /**
    * Creates a static hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param type
    *           the type of the search keys
    * @param numBuckets
    *           number of buckets the index has
    * @return an empty persistent linear hash index
    */
   private static StaticHashIndex createIndex(final BufferManager bufferManager,
         final Optional<String> fileName, final SearchKeyType type, final int numBuckets) {
      final Page<HashDirectoryHeader> header = HashDirectoryHeader.newPage(bufferManager, numBuckets,
            Float.NaN, Float.NaN, type);
      final PageID headID = header.getPageID();

      // fill the directory
      Page<? extends HashDirectoryPage> dirPage = header;
      int capacity = HashDirectoryHeader.getWritableSize(type) / PageID.BYTES;
      int pos = 0;
      for (int i = 0; i < numBuckets; i++) {
         if (pos >= capacity) {
            final Page<HashDirectoryPage> newPage = HashDirectoryPage.newPage(bufferManager);
            HashDirectoryPage.setNextPagePointer(dirPage, newPage.getPageID());
            bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
            dirPage = newPage;
            capacity = HashDirectoryPage.WRITABLE_SIZE / PageID.BYTES;
            pos = 0;
         }
         HashDirectoryPage.writePageID(dirPage, pos++, PageID.INVALID);
      }
      bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);

      fileName.ifPresent(x -> bufferManager.getDiskManager().addFileEntry(x, headID));
      return new StaticHashIndex(bufferManager, fileName, headID, numBuckets, 0, type);
   }

   /**
    * Creates a temporary static hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param type
    *           the type of the search keys
    * @param depth
    *           depth of the index
    * @return an empty temporary linear hash index
    */
   public static StaticHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final SearchKeyType type, final byte depth) {
      return createIndex(bufferManager, Optional.empty(), type, depth);
   }

   /**
    * Creates a temporary static hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param type
    *           the type of the search keys
    * @return an empty temporary linear hash index
    */
   public static StaticHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final SearchKeyType type) {
      return createIndex(bufferManager, Optional.empty(), type, DEFAULT_SIZE);
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
    *           number of entries currently stored in this index
    * @param type
    *           the type of the search keys
    */
   private StaticHashIndex(final BufferManager bufferManager, final Optional<String> fileName,
         final PageID headID, final int numBuckets, final int numEntries, final SearchKeyType type) {
      super(bufferManager, fileName, headID, numBuckets, numEntries, type);
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(rid);
      if (!rid.getPageID().isValid()) {
         throw new IllegalArgumentException("The record ID's page ID must be valid.");
      }
      final int entrySize = this.getEntrySize();
      final IndexEntry entry = new IndexEntry(key, rid, this.getKeyType());

      // search dataPage for key and insert
      final BufferManager bufferManager = this.getBufferManager();
      final int hash = this.getBucketNo(key);
      final Page<BucketPage> bucketPage;
      int offset = hash;
      for (PageID dirID = this.getHeadID();;) {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int numEntries = this.capacity(dirPage);
         if (offset < numEntries) {
            final PageID pageID = HashDirectoryPage.readPageID(dirPage, offset);
            if (pageID.isValid()) {
               bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               bucketPage = bufferManager.pinPage(pageID);
            } else {
               bucketPage = BucketPage.newPage(bufferManager);
               HashDirectoryPage.writePageID(dirPage, offset, bucketPage.getPageID());
               bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
            }
            break;
         }
         offset -= numEntries;
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      if (!BucketPage.hasSpaceLeft(bucketPage, entrySize)) {
         // move the contents of the first page to the next one so that the it stays partially filled
         // and the entries in the directory stay valid
         final Page<BucketPage> overflow = bufferManager.copyPage(bucketPage);
         final PageID overflowID = overflow.getPageID();
         bufferManager.unpinPage(overflow, UnpinMode.DIRTY);
         BucketPage.setOverflowPointer(bucketPage, overflowID);
         BucketPage.setNumEntries(bucketPage, 0);
      }

      BucketPage.appendDataEntry(bucketPage, entry, entrySize);
      bufferManager.unpinPage(bucketPage, UnpinMode.DIRTY);
      this.updateNumEntries(1);
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {
      // find the bucket in the directory
      final IndexEntry entry =
            new IndexEntry(Objects.requireNonNull(key), Objects.requireNonNull(rid), this.getKeyType());
      final BufferManager bufferManager = this.getBufferManager();

      final PageID firstPageID;
      PageID dirID = this.getHeadID();
      int offset = this.getBucketNo(key);
      for (;;) {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int numEntries = this.capacity(dirPage);
         if (offset < numEntries) {
            firstPageID = HashDirectoryPage.readPageID(dirPage, offset);
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            if (!firstPageID.isValid()) {
               // bucket is empty
               return false;
            }
            break;
         }
         offset -= numEntries;
         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      // iterate over bucket chain
      Page<BucketPage> bucketPage = bufferManager.pinPage(firstPageID);
      final int onFirstPage = BucketPage.getNumEntries(bucketPage);
      int onPage = onFirstPage;
      int entryPos = -1;
      for (;;) {
         // iterate over every entry on the page
         for (int i = 0; i < onPage; i++) {
            final IndexEntry e = BucketPage.readDataEntryAt(bucketPage, i, entry.getKeyType());
            if (entry.equals(e)) {
               entryPos = i;
               break;
            }
         }
         if (entryPos >= 0) {
            break;
         }

         // move along the chain
         final PageID nextID = BucketPage.getNextPagePointer(bucketPage);
         bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
         if (!nextID.isValid()) {
            // entry is not present
            return false;
         }
         bucketPage = bufferManager.pinPage(nextID);
         onPage = BucketPage.getNumEntries(bucketPage);
      }

      // delete the entry from the bucket page
      final Page<BucketPage> firstPage = bufferManager.pinPage(firstPageID);
      if (bucketPage.getPageID().equals(firstPageID)) {
         // we are still on the primary page of the bucket
         BucketPage.removeDataEntryAt(bucketPage, entryPos, this.getKeyType());
      } else {
         // we are on an overflow page, need to delete the entry there and get a new entry
         // from the primary page so our invariant that overflow pages are always full still holds
         BucketPage.moveLastEntryTo(firstPage, bucketPage, entryPos, this.getKeyType());
      }
      bufferManager.unpinPage(bucketPage, UnpinMode.DIRTY);

      // check if first page is now empty and chain needs to be shrunk
      if (onFirstPage == 1) {
         final PageID firstNextID = BucketPage.getNextPagePointer(firstPage);
         bufferManager.freePage(firstPage);
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         HashDirectoryPage.writePageID(dirPage, offset, firstNextID);
         bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
      } else {
         bufferManager.unpinPage(firstPage, UnpinMode.DIRTY);
      }

      this.updateNumEntries(-1);
      return true;
   }

   @Override
   public String prettyPrint() {
      // append filename
      final StringBuilder sb = new StringBuilder(this.getFileName().orElse("(temp)")).append("\n");
      for (int i = 0; i < this.getFileName().orElse("(temp)").length(); i++) {
         sb.append('-');
      }
      sb.append("\n");

      // append directory listing
      // show which buckets are allocated and how many entries each bucket contains

      int currBucketNum = 0;

      int totalEntries = 0;

      // append buckets stored on directory pages
      final BufferManager bufferManager = this.getBufferManager();
      PageID dirID = this.getHeadID();
      int numDirPages = 0;
      final int digits = Integer.toString(this.getNumBuckets() - 1).length();
      while (dirID.isValid()) {
         numDirPages++;
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         // we do not want to read more slots than we have left or that are on the page
         final int dirBuckets = Math.min(this.getNumBuckets() - currBucketNum, this.capacity(dirPage));
         for (int i = 0; i < dirBuckets; ++i) {
            final String hash = Integer.toString(currBucketNum, 2);
            for (int j = hash.length(); j < digits; j++) {
               sb.append('0');
            }
            sb.append(hash + " : ");
            final PageID bucketID = HashDirectoryPage.readPageID(dirPage, i);
            if (bucketID.isValid()) {
               final Page<BucketPage> page = bufferManager.pinPage(bucketID);
               int bucketEntries = BucketPage.getNumEntries(page);
               PageID nextID = BucketPage.getNextPagePointer(page);
               while (nextID.isValid()) {
                  final Page<BucketPage> bucketPage = bufferManager.pinPage(nextID);
                  bucketEntries += BucketPage.getNumEntries(bucketPage);
                  nextID = BucketPage.getNextPagePointer(bucketPage);
                  bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
               }
               totalEntries += bucketEntries;
               bufferManager.unpinPage(page, UnpinMode.CLEAN);
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
      sb.append("Total # entries (counted): " + this.getNumEntries() + " (" + totalEntries + ")\n");
      sb.append("Total # buckets (counted): " + this.getNumBuckets() + " (" + currBucketNum + ")\n");
      sb.append("# directory pages: " + numDirPages + "\n");
      return sb.toString();
   }

   @Override
   public void checkInvariants() {
      final BufferManager bufferManager = this.getBufferManager();
      final int numBuckets = this.getNumBuckets();
      int entries = 0;
      PageID dirID = this.getHeadID();
      int seen = 0;
      do {
         final Page<HashDirectoryPage> dirPage = bufferManager.pinPage(dirID);
         final int cap = this.capacity(dirPage);
         final int onPage = Math.min(cap, numBuckets - seen);
         if (onPage == 0) {
            bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
            throw new AssertionError("Directory too long.");
         }

         for (int i = 0; i < onPage; i++) {
            final int hash = seen + i;
            PageID bucketID = HashDirectoryPage.readPageID(dirPage, i);
            while (bucketID.isValid()) {
               final Page<BucketPage> bucketPage = bufferManager.pinPage(bucketID);
               final int onBucketPage = BucketPage.getNumEntries(bucketPage);
               for (int j = 0; j < onBucketPage; j++) {
                  final SearchKey key = BucketPage.readSearchKey(bucketPage, j, this.getKeyType());
                  if (this.getBucketNo(key) != hash) {
                     bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
                     bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
                     throw new AssertionError("Wrong search key in bucket " + hash + ": " + key);
                  }
               }

               final PageID nextID = BucketPage.getNextPagePointer(bucketPage);
               bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
               if (onBucketPage == 0) {
                  bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
                  throw new AssertionError("Empty bucket page in bucket " + hash + ": " + bucketID);
               }
               entries += onBucketPage;
               bucketID = nextID;
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

   @Override
   void writeAdditionalMetadata(final Page<HashDirectoryHeader> header) {
   }

   @Override
   int getBucketNo(final SearchKey key) {
      return Math.floorMod(key.hashCode(), this.getNumBuckets());
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

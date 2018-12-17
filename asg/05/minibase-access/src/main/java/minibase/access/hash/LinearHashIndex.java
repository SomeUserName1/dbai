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

import java.util.Iterator;
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
 * This unclustered index implements linear hashing as in the textbook (3rd edition).
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni-konstanz.de&gt;
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 */
public final class LinearHashIndex implements HashIndex {

   /** File name of the hash index, {@link Optional#empty} for temporary indexes. */
   private final Optional<String> fileName;
   /** ID of the header page. */
   private PageID headID;

   /** Current size in buckets. */
   private int numBuckets;
   /** Current number of entries in the index. */
   private int numEntries;

   /** Default initial size of the index in number of buckets. */
   private static final int DEFAULT_MIN_NUM_BUCKETS = 1;
   /** Minimum number of buckets. */
   private final int minNumBuckets;
   /** Default minimum load factor. */
   private static final float DEFAULT_MIN_LOAD_FACTOR = 0.4f;
   /** Minimum load factor. */
   private final float minLoadFactor;
   /** Default maximum load factor. */
   private static final float DEFAULT_MAX_LOAD_FACTOR = 0.8f;
   /** Maximum load factor. */
   private final float maxLoadFactor;

   /** Reference to the buffer manager. */
   private final BufferManager bufferManager;

   /** Size of a {@link DataEntry} stored in the index. */
   private final int entrySize;

   /**
    * Opens an existing linear hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an existing linear hash index
    */
   public static LinearHashIndex openIndex(final BufferManager bufferManager, final String fileName,
         final int searchKeyLength) {
      final PageID headID = bufferManager.getDiskManager().getFileEntry(fileName);
      if (!headID.isValid()) {
         return null;
      }
      return new LinearHashIndex(bufferManager, Optional.of(fileName), headID);
   }

   /**
    * Creates a persistent linear hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty persistent linear hash index
    */
   public static LinearHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final int searchKeyLength) {

      final PageID headID = initializeHeaderPage(bufferManager, DEFAULT_MIN_NUM_BUCKETS, DEFAULT_MIN_LOAD_FACTOR,
              DEFAULT_MAX_LOAD_FACTOR, searchKeyLength);
      initializeDirectory(bufferManager, headID);

      bufferManager.getDiskManager().addFileEntry(fileName, headID);
      return new LinearHashIndex(bufferManager, Optional.of(fileName), headID);
   }

   /**
    * Creates a temporary linear hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty temporary linear hash index
    */
   public static LinearHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final int searchKeyLength) {
      final PageID headID = initializeHeaderPage(bufferManager, DEFAULT_MIN_NUM_BUCKETS, DEFAULT_MIN_LOAD_FACTOR,
              DEFAULT_MAX_LOAD_FACTOR, searchKeyLength);
      initializeDirectory(bufferManager, headID);

      return new LinearHashIndex(bufferManager, Optional.empty(), headID);
   }

   /**
    * Creates a temporary linear hash index. Note: although it is a temporary index, you have to delete the
    * index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param searchKeyLength
    *           maximum length of the search key
    * @param minNumBuckets
    *           minimum number of buckets
    * @param minLoadFactor
    *           minimum load factor
    * @param maxLoadFactor
    *           maximum load factor
    * @return an empty temporary linear hash index
    */
   public static LinearHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final int searchKeyLength, final int minNumBuckets, final float minLoadFactor,
         final float maxLoadFactor) {
      final PageID headID = initializeHeaderPage(bufferManager, minNumBuckets,
              minLoadFactor, maxLoadFactor, searchKeyLength);
      initializeDirectory(bufferManager, headID);

      return new LinearHashIndex(bufferManager, Optional.empty(), headID);
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
    */
   private LinearHashIndex(final BufferManager bufferManager, final Optional<String> fileName,
         final PageID headID) {
      this.bufferManager = bufferManager;
      this.fileName = fileName;
      this.headID = headID;
      // initialize from header
      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      this.numEntries = HashDirectoryHeader.getSize(header);
      this.numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      this.minNumBuckets = HashDirectoryHeader.getNumberBuckets(header);
      this.minLoadFactor = HashDirectoryHeader.getMinLoadFactor(header);
      this.maxLoadFactor = HashDirectoryHeader.getMaxLoadFactor(header);
      this.entrySize = HashDirectoryHeader.getEntrySize(header);
      this.bufferManager.unpinPage(header, UnpinMode.CLEAN);
   }

   @Override
   public void delete() {
      // load the header
      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      int numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      this.bufferManager.unpinPage(header, UnpinMode.CLEAN);
      Page<HashDirectoryPage> page = this.bufferManager.pinPage(this.headID);
      int pos = 0;

      // iterate the directory
      while (numBuckets > 0) {
         if (pos >= LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page))) {
            final Page<HashDirectoryPage> nextPage =
                    this.bufferManager.pinPage(HashDirectoryPage.getNextPagePointer(page));
            this.bufferManager.freePage(page);
            page = nextPage;
            pos = 0;
         }
         // delete the whole bucket
         final PageID pageID = HashDirectoryPage.readPageID(page, pos);
         if (pageID.isValid()) {
            // free the whole bucket
            Page<BucketPage> bucketPage = this.bufferManager.pinPage(pageID);
            PageID nextBucketPageId = BucketPage.getNextPagePointer(bucketPage);
            while (nextBucketPageId.isValid()) {
               final Page<BucketPage> nextBucketPage = this.bufferManager.pinPage(nextBucketPageId);
               this.bufferManager.freePage(bucketPage);

               bucketPage = nextBucketPage;
               nextBucketPageId = BucketPage.getNextPagePointer(bucketPage);
            }
            this.bufferManager.freePage(bucketPage);
         }
         --numBuckets;
         pos++;
      }

      this.bufferManager.freePage(page);

      if (this.fileName.isPresent()) {
         this.bufferManager.getDiskManager().deleteFileEntry(this.fileName.get());
      }
      this.headID = PageID.INVALID;
      this.numBuckets = 0;
   }

   @Override
   public Optional<DataEntry> search(final SearchKey key) {
      // get the right bucket
      final int hash = key.getHash(this.getLevel());
      final PageID bucketId = this.getBucketID(hash);

      // check if the bucket is valid
      if (!bucketId.isValid()) {
         return Optional.empty();
      }

      // iterate the bucket
      final BucketChainIterator iterator = new BucketChainIterator(this.bufferManager, bucketId, this.entrySize);
      while (iterator.hasNext()) {
         final DataEntry entry = iterator.next();
         if (entry.getSearchKey().equals(key)) {
            return Optional.of(entry);
         }
      }

      return Optional.empty();
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {

      // check the arguments
      if (key == null || rid == null) {
         throw new NullPointerException();
      }
      if (!rid.getPageID().isValid()) {
         throw new IllegalThreadStateException();
      }

      // get the bucket in insert into
      final int hash = key.getHash(this.getLevel());
      PageID bucketId = this.getBucketID(hash);

      // insert the value
      final PageID insertedBucketId = this.insertIntoBucket(bucketId, new DataEntry(key, rid, this.entrySize));

      // check if a new bucket was created by the insert method
      if (!bucketId.isValid() || !bucketId.equals(insertedBucketId)) {
         bucketId = insertedBucketId;
         this.setBucketID(hash, bucketId);
      }


      // update header
      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      HashDirectoryHeader.setSize(header, ++this.numEntries);
      this.bufferManager.unpinPage(header, UnpinMode.DIRTY);

      // check if a split is needed
      if (this.calcLoadFactor() > this.maxLoadFactor) {

         // create two new buckets
         final Page<BucketPage> lowerBucket = BucketPage.newPage(this.bufferManager);
         final Page<BucketPage> upperBucket = BucketPage.newPage(this.bufferManager);
         final PageID lowerBucketId = lowerBucket.getPageID();
         final PageID upperBucketId = upperBucket.getPageID();

         PageID oldId = this.getBucketID(this.numBuckets);
         if (!oldId.isValid()) {
            final Page<BucketPage> newPage = BucketPage.newPage(this.bufferManager);
            oldId = newPage.getPageID();
            this.bufferManager.unpinPage(newPage, UnpinMode.DIRTY);
         }

         // split the bucket
         final BucketChainIterator iterator = new BucketChainIterator(this.bufferManager, oldId, this.entrySize);
         final int level = this.getNextLevel();
         while (iterator.hasNext()) {
            final DataEntry entry = iterator.next();

            if (entry.getSearchKey().getHash(level) < this.numBuckets) {
               this.insertIntoBucket(lowerBucketId, entry);
            } else {
               this.insertIntoBucket(upperBucketId, entry);
            }
         }

         // delete the old bucket
         Page<BucketPage> bucketPage = this.bufferManager.pinPage(oldId);
         oldId = BucketPage.getNextPagePointer(bucketPage);
         while (oldId.isValid()) {
            final Page<BucketPage> nextPage = this.bufferManager.pinPage(oldId);
            this.bufferManager.freePage(bucketPage);

            bucketPage = nextPage;
            oldId =  BucketPage.getNextPagePointer(bucketPage);
         }
         this.bufferManager.freePage(bucketPage);

         // set the new bucket ids in the directory
         if (BucketPage.getNumEntries(lowerBucket) != 0) {
            this.setBucketID(this.numBuckets, lowerBucketId);
            this.bufferManager.unpinPage(lowerBucket, UnpinMode.DIRTY);
         } else {
            this.setBucketID(this.numBuckets, PageID.INVALID);
            this.bufferManager.freePage(lowerBucket);
         }

         if (BucketPage.getNumEntries(upperBucket) != 0) {
            this.addBucket(upperBucketId);
            this.bufferManager.unpinPage(upperBucket, UnpinMode.DIRTY);
         } else {
            this.addBucket(PageID.INVALID);
            this.bufferManager.freePage(upperBucket);
         }
      }
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {

      // check the arguments
      if (key == null || rid == null) {
         throw new NullPointerException();
      }
      final int hash = key.getHash(this.getLevel());

      // remove the entry and check if it was successful
      if (this.removeFromBucket(hash, new DataEntry(key, rid, this.entrySize))) {
         final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
         HashDirectoryHeader.setSize(header, --this.numEntries);
         this.bufferManager.unpinPage(header, UnpinMode.DIRTY);

         // check if a merge is needed
         if (this.calcLoadFactor() < this.minLoadFactor && this.numBuckets > 1) {

            // remove the last bucket
            final PageID bucketHeadId = this.removeBucket();
            if (bucketHeadId.isValid()) {

               // get the bucket to merge into
               PageID mergeBucketId = this.getBucketID(this.numBuckets);

               if (!mergeBucketId.isValid()) {
                  final Page<BucketPage> newBucket = BucketPage.newPage(this.bufferManager);
                  mergeBucketId = newBucket.getPageID();
                  this.setBucketID(this.numBuckets, mergeBucketId);
                  this.bufferManager.unpinPage(newBucket, UnpinMode.DIRTY);
               }

               // merge the buckets
               final BucketChainIterator iterator =
                       new BucketChainIterator(this.bufferManager, bucketHeadId, this.entrySize);
               while (iterator.hasNext()) {
                  final DataEntry entry = iterator.next();

                  this.insertIntoBucket(mergeBucketId, entry);
               }

               // delete the pages of the merged bucket
               Page<BucketPage> bucketPage = this.bufferManager.pinPage(bucketHeadId);
               PageID nextPageId = BucketPage.getNextPagePointer(bucketPage);

               while (nextPageId.isValid()) {
                  final Page<BucketPage> nextPage = this.bufferManager.pinPage(nextPageId);
                  this.bufferManager.freePage(bucketPage);

                  bucketPage = nextPage;
                  nextPageId = BucketPage.getNextPagePointer(bucketPage);
               }

               this.bufferManager.freePage(bucketPage);
            }
         }
         return true;
      } else {
         return false;
      }
   }

   @Override
   public HashIndexScan openScan() {
      return new HashIndexScan(this, this.bufferManager, this.entrySize);
   }

   @Override
   public IndexScan openScan(final SearchKey key) {
      return new KeyScan(this.bufferManager, key, this.getBucketID(key.getHash(this.getLevel())), this.entrySize);
   }

   /**
    * Appends a high-level view of the directory, namely which buckets are allocated and how many entries are
    * stored in each one, to the given string builder. Sample output:
    *
    * <pre>
    *   IX_Customers
    *   ------------
    *   0000000 : 35
    *   0000001 : 39
    *   0000010 : 27
    *   ...
    *   1111111 : 42
    *   ------------
    *   Total : 1500
    * </pre>
    *
    * @param sb
    *           string builder to append the print results to
    * @return the appended string builder
    */
   public StringBuilder prettyPrint(final StringBuilder sb) {

      // append filename
      final String name = this.fileName.orElse("(temp)");
      sb.append(name);
      sb.append("\n");
      for (int i = 0; i < name.length(); i++) {
         sb.append('-');
      }
      sb.append("\n");

      // append directory listing
      // show which buckets are allocated and how many entries each bucket contains

      int currBucketNum = 0;
      int totalEntries = 0;

      // append buckets stored on directory pages
      PageID dirID = this.headID;
      int numDirPages = 0;
      while (dirID.isValid()) {
         numDirPages++;
         final Page<HashDirectoryPage> dirPage = this.bufferManager.pinPage(dirID);
         // we do not want to read more slots than we have left or that are on the page
         final int dirBuckets = Math.min(this.numBuckets - currBucketNum,
               LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(dirPage)));
         for (int i = 0; i < dirBuckets; ++i) {
            final String hash = Integer.toString(currBucketNum, 2);
            for (int j = 0; j < this.getLevel() - hash.length() + 1; ++j) {
               sb.append('0');
            }
            sb.append(hash + " : ");
            PageID bucketID = HashDirectoryPage.readPageID(dirPage, i);

            if (bucketID.isValid()) {
               sb.append('[');
               final BucketChainIterator it = new BucketChainIterator(this.bufferManager, bucketID, this.entrySize);
               while (it.hasNext()) {
                  sb.append(it.next().getSearchKey().toString());
                  if (it.hasNext()) {
                     sb.append(", ");
                  }
               }
               sb.append("] ");
            }

            if (bucketID.isValid()) {
               final Page<BucketPage> page = this.bufferManager.pinPage(bucketID);
               final int bucketEntries = BucketPage.computeOnChain(this.bufferManager, page,
                     BucketPage::getNumEntries, (final Integer a, final Integer b) -> a + b);
               totalEntries += bucketEntries;
               bucketID = BucketPage.getNextPagePointer(page);
               this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
               sb.append(bucketEntries);
            } else {
               sb.append("null");
            }
            sb.append("\n");
            currBucketNum++;
         }

         dirID = HashDirectoryPage.getNextPagePointer(dirPage);
         this.bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      for (int j = 0; j < name.length(); ++j) {
         sb.append('-');
      }
      sb.append("\n");
      sb.append("Total # entries (counted): " + this.numEntries + " (" + totalEntries + ")" + "\n");
      sb.append("Total # buckets (counted): " + this.numBuckets + " (" + currBucketNum + ")" + "\n");
      sb.append("# directory pages: " + numDirPages + "\n");
      return sb;
   }

   /**
    * Prints a high-level view of the directory, namely which buckets are allocated and how many entries are
    * stored in each one. Sample output:
    *
    * <pre>
    *   IX_Customers
    *   ------------
    *   0000000 : 35
    *   0000001 : 39
    *   0000010 : 27
    *   ...
    *   1111111 : 42
    *   ------------
    *   Total : 1500
    * </pre>
    */
   @Override
   public void printSummary() {
      final StringBuilder sb = new StringBuilder();
      this.prettyPrint(sb);
      System.out.println(sb.toString());
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
   public Iterator<PageID> directoryIterator() {
      return new HashDirectoryIterator(this.bufferManager, this.headID);
   }

   @Override
   public BufferManager getBufferManager() {
      // TODO remove
      throw new AssertionError("Will not be implemented.");
   }

   @Override
   public int getDepth() {
      // TODO remove
      throw new AssertionError("Will not be implemented.");
   }

   @Override
   public PageID getHeadID() {
      // TODO remove
      throw new AssertionError("Will not be implemented.");
   }

   /**
    * Gets the current level for hashing based on the number of buckets in the index.
    *
    * @return the current level
    */
   private int getNextLevel() {
      this.numBuckets++;
      final int level = this.getLevel();
      this.numBuckets--;
      return level;
   }

   /**
    * Gets the level for hashing based on the number of buckets in the index after adding the next bucket.
    *
    * @return the current level
    */
   private int getLevel() {
      // this gives us the lowest power of two possible with the current number of buckets
      return this.calcLevel(this.numBuckets);
   }

   /**
    *
    * @param numBuckets current number of buckets
    * @return the level of the linear hash index
    */
   private int calcLevel(final int numBuckets) {
      return 31 - Integer.numberOfLeadingZeros((numBuckets << 1) - 1);
   }

   /**
    * Calculates the maximum number of PageID's given the writable size.
    * @param writableSize writable size
    * @return number of Page ID's
    */
   private static int calcNumPageIDPerPage(final short writableSize) {
       return writableSize / PageID.SIZE;
   }

   /**
    * This Method initializes a new HeaderPage.
    *
    * @param bufferManager
    *          buffer manager
    * @param minNumBuckets min number of buckets
    * @param minLoadFactor minimum load factor
    * @param maxLoadFactor maximum load factor
    * @param searchKeyLength
    *          max search key length
    * @return
    *          page id of created header page
    */
   private static PageID initializeHeaderPage(final BufferManager bufferManager, final int minNumBuckets,
                                                    final float minLoadFactor, final float maxLoadFactor,
                                                    final int searchKeyLength) {
      final Page<HashDirectoryHeader> header = HashDirectoryHeader.newPage(bufferManager, minNumBuckets,
               minLoadFactor, maxLoadFactor, DataEntry.getLength(searchKeyLength));
      final PageID headID = header.getPageID();
      bufferManager.unpinPage(header, UnpinMode.DIRTY);
      return headID;
   }

   /**
    * This Method initializes the Directory.
    *
    * @param bufferManager
    *          buffer manager
    * @param headID
    *          id of header page
    */
   private static void initializeDirectory(final BufferManager bufferManager, final PageID headID) {

      // read number of buckets
      final Page<HashDirectoryHeader> header = bufferManager.pinPage(headID);
      int numBuckets = HashDirectoryHeader.getNumberBuckets(header);
      bufferManager.unpinPage(header, UnpinMode.CLEAN);
      Page<HashDirectoryPage> page = bufferManager.pinPage(headID);
      int pos = 0;

      // init directory
      while (numBuckets > 0) {
         if (pos >= LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page))) {
            final Page<HashDirectoryPage> newPage = HashDirectoryPage.newPage(bufferManager);
            HashDirectoryPage.setNextPagePointer(page, newPage.getPageID());
            bufferManager.unpinPage(page, UnpinMode.DIRTY);
            page = newPage;
            pos = 0;
         }
         HashDirectoryPage.writePageID(page, pos, PageID.INVALID);
         --numBuckets;
         pos++;
      }

      bufferManager.unpinPage(page, UnpinMode.DIRTY);
   }


   /**
    * get the PageID for a given hash.
    *
    * @param hash
    *          the hash to return the PageID for
    * @return
    *          PageID
    */
   private PageID getBucketID(int hash) {

      // if the hash is greater than the number of buckets ignore the highest digit of the hash
      if (hash >= this.numBuckets) {
         hash &= ((0x1 << this.getLevel()) - 1) >> 1;
      }

      // search the index in the directory
      Page<HashDirectoryPage> page = this.bufferManager.pinPage(this.headID);
      int pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));

      while (hash >= pageSize) {

         hash -= pageSize;
         final Page<HashDirectoryPage> newPage = this.bufferManager.pinPage(HashDirectoryPage.getNextPagePointer(page));
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

         page = newPage;
         pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
      }

      // get the value
      final PageID pageId = HashDirectoryPage.readPageID(page, hash);
      this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

      return pageId;
   }

   /**
    * this method sets a PageID in the Directory.
    *
    * @param hash
    *          the hash to set the PageID for
    * @param pageId
    *          the PageID to set for the hash
    */
   private void setBucketID(int hash, final PageID pageId) {

      // if the hash is greater than the number of buckets ignore the highest digit of the hash
      if (hash >= this.numBuckets) {
         hash &= ((0x1 << this.getLevel()) - 1) >> 1;
      }

      // search the index in the directory
      Page<HashDirectoryPage> page = this.bufferManager.pinPage(this.headID);
      int pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));

      while (hash >= pageSize) {

         hash -= pageSize;
         final Page<HashDirectoryPage> newPage = this.bufferManager.pinPage(HashDirectoryPage.getNextPagePointer(page));
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

         page = newPage;
         pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
      }

      // replace the value and save it
      HashDirectoryPage.writePageID(page, hash, pageId);
      this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
   }

   /**
    * this method adds a bucket to the end of the index.
    *
    * @param pageID the page id
    */
   private void addBucket(final PageID pageID) {

      // the new bucket should be inserted at index numBuckets
      int hash = this.numBuckets;

      Page<HashDirectoryPage> page = this.bufferManager.pinPage(this.headID);

      int pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));

      // as long as the has is greater than the number of entries on the page the entry must be on the next page
      while (hash > pageSize) {

         hash -= pageSize;
         final Page<HashDirectoryPage> nextPage =
                 this.bufferManager.pinPage(HashDirectoryPage.getNextPagePointer(page));
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

         page = nextPage;
         pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
      }

      if (hash < pageSize) {
         HashDirectoryPage.writePageID(page, hash, pageID);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
      } else {
         // the inserted bucket needs a new directory page
         final Page<HashDirectoryPage> newPage = HashDirectoryPage.newPage(this.bufferManager);
         HashDirectoryPage.setNextPagePointer(page, newPage.getPageID());
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
         page = newPage;

         HashDirectoryPage.writePageID(page, 0, pageID);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
      }

      // update header
      this.numBuckets++;
      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      HashDirectoryHeader.setNumberBuckets(header, this.numBuckets);
      this.bufferManager.unpinPage(header, UnpinMode.DIRTY);
   }

   /**
    * removes the last bucket from the directory.
    *
    * @return
    *          the PageID of the removed bucket
    */
   private PageID removeBucket() {

      // the last bucket is at index numBuckets - 1
      int hash = this.numBuckets - 1;

      // pin header
      Page<HashDirectoryPage> page = this.bufferManager.pinPage(this.headID);

      // get the numbers of entries in the page
      int pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));

      // as long as the hash in greater then the number of entries in the page the entry we are looking for must
      // be in the next page
      while (hash > pageSize) {

         hash -= pageSize;
         final Page<HashDirectoryPage> nextPage =
                 this.bufferManager.pinPage(HashDirectoryPage.getNextPagePointer(page));
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

         page = nextPage;
         pageSize = LinearHashIndex.calcNumPageIDPerPage(HashDirectoryPage.getWritableSize(page));
      }

      final PageID bucketId;

      if (hash < pageSize) {
         bucketId = HashDirectoryPage.readPageID(page, hash);
         HashDirectoryPage.writePageID(page, hash, PageID.INVALID);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
      } else {

         // the removed entry is the only one in the page. delete it.
         final Page<HashDirectoryPage> nextPage =
                 this.bufferManager.pinPage(HashDirectoryPage.getNextPagePointer(page));
         HashDirectoryPage.setNextPagePointer(page, PageID.INVALID);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);

         page = nextPage;
         bucketId = HashDirectoryPage.readPageID(page, 0);
         this.bufferManager.freePage(page);
      }

      // update header
      this.numBuckets--;
      final Page<HashDirectoryHeader> header = this.bufferManager.pinPage(this.headID);
      HashDirectoryHeader.setNumberBuckets(header, this.numBuckets);
      this.bufferManager.unpinPage(header, UnpinMode.DIRTY);

      return bucketId;
   }

   /**
    * Tries to insert entry into bucket. (copied from StaticHashIndex.java)
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


   /**
    * Deletes the given entry from the bucket. (copied from StaticHashIndex.java)
    *
    * @param hash
    *           hash of the bucket to delete from
    * @param entry
    *           entry to delete
    * @return {@code true} if the entry was deleted, {@code false} otherwise
    */
   private boolean removeFromBucket(final int hash, final DataEntry entry) {
      final PageID firstPageID = this.getBucketID(hash);
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
               BucketPage.removeDataEntryAt(deleteFrom, i, this.entrySize);
            } else {
               // we are on an overflow page, need to delete the entry there and get a new entry
               // from the primary page so our invariant that overflow pages are always full still holds
               BucketPage.moveLastEntryTo(primaryPage, deleteFrom, i, this.entrySize);
            }
            // the overflow page is now full again
            this.bufferManager.unpinPage(deleteFrom, UnpinMode.DIRTY);
            // check if primary page is now empty and chain needs to be shrunk
            if (BucketPage.getNumEntries(primaryPage) == 0) {
               final PageID nextID = BucketPage.getNextPagePointer(primaryPage);
               this.bufferManager.freePage(primaryPage);
               this.setBucketID(hash, nextID);
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

   /**
    * calculates the current load factor.
    *
    * @return
    *          the current load factor
    */
   private float calcLoadFactor() {
      // formula from the exercise sheet
      return ((float) this.numEntries)
              / ((float) this.numBuckets * (float) BucketPage.maxNumEntries(this.entrySize) / (float) this.entrySize);
   }
}

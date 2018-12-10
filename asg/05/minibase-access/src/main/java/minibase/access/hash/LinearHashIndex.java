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
      // TODO Implement this method!
      return null;
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
      // TODO Implement this method!
      return null;
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
      // TODO Implement this method!
      return null;
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
      // TODO Implement this method!
   }

   @Override
   public Optional<DataEntry> search(final SearchKey key) {
      // TODO Implement this method!
      return null;
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {
      // TODO Implement this method!
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {
      // TODO Implement this method!
      return true;
   }

   @Override
   public HashIndexScan openScan() {
      // TODO Implement this method!
      return null;
   }

   @Override
   public IndexScan openScan(final SearchKey key) {
      // TODO Implement this method!
      return null;
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
   private int getLevel() {
      // this gives us the lowest power of two possible with the current number of buckets
      return 31 - Integer.numberOfLeadingZeros(this.numBuckets);
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

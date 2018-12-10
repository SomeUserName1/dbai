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
 * This is a simple implementation of a extendible HashIndex. It starts with one bucket and grows as needed.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
public class ExtendibleHashIndex implements HashIndex {

   /** Invalid Number. */
   private static final int INVALID = -1;

   /** File name of the hash index. */
   private final Optional<String> fileName;

   /** First page id of the directory. */
   private PageID headID;

   /** Global depth of the hash index. */
   private int depth;

   /** Number of buckets of the hash index. */
   private int numBuckets;

   /** Number of Entries in the index. */
   private int numEntries;

   /** Reference to the buffer manager. */
   private final BufferManager bufferManager;

   /** Size of a {@link DataEntry} stored in the index. */
   private final int entrySize;

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
      return new ExtendibleHashIndex(bufferManager, Optional.of(fileName), headID);
   }

   /**
    * Creates a persistent extendible hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty persistent linear hash index
    */
   public static ExtendibleHashIndex createIndex(final BufferManager bufferManager, final String fileName,
         final int searchKeyLength) {
      return createIndex(bufferManager, Optional.of(fileName), searchKeyLength);
   }

   /**
    * Creates a temporary extendible hash index. Note: although it is a temporary index, you have to delete
    * the index yourself.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty temporary linear hash index
    */
   public static ExtendibleHashIndex createTemporaryIndex(final BufferManager bufferManager,
         final int searchKeyLength) {
      return createIndex(bufferManager, Optional.empty(), searchKeyLength);
   }

   /**
    * Creates a extendible hash index.
    *
    * @param bufferManager
    *           buffer manager to use
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @return an empty persistent linear hash index
    */
   private static ExtendibleHashIndex createIndex(final BufferManager bufferManager,
         final Optional<String> fileName, final int searchKeyLength) {
      // TODO Implement this method!
      return null;
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
   public ExtendibleHashIndex(final BufferManager bufferManager, final Optional<String> fileName,
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


   @Override
   public void delete() {
      // TODO Implement this method.
   }

   @Override
   public void insert(final SearchKey key, final RecordID rid) {
      // TODO Implement this method.
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID rid) {
      // TODO Implement this method.
      return true;
   }


   @Override
   public Optional<DataEntry> search(final SearchKey key) {
      // TODO Implement this method.
      return null;
   }

   @Override
   public HashIndexScan openScan() {
      // TODO Implement this method.
      return null;
   }

   @Override
   public IndexScan openScan(final SearchKey key) {
      // TODO Implement this method.
      return null;
   }

   /** BufferManager. */
   @Override
   public BufferManager getBufferManager() {
      // TODO Implement this method.
      return null;
   }

   /** Get depth. */
   @Override
   public int getDepth() {
      // TODO Implement this method.
      return 0;
   }

   /** Get head id. */
   @Override
   public PageID getHeadID() {
      // TODO Implement this method.
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
      final String name = this.fileName.orElse("Temporary Index");
      sb.append(name);
      sb.append("\n");
      for (int i = 0; i < name.length(); i++) {
         sb.append('-');
      }
      sb.append("\n");

      int totalEntries = 0;
      int currBucketNum = 0;
      int numBktAlloc = 0;
      int numDirPages = 0;
      // append directory listing
      // show which buckets are allocated and how many entries each bucket contains
      PageID pageID = this.headID;
      while (pageID.isValid()) {
         final Page<HashDirectoryPage> page = this.bufferManager.pinPage(pageID);
         final int numEntries = ExtendibleHashIndex
               .calcNumEntriesPerPage(HashDirectoryPage.getWritableSize(page));
         for (int i = 0; i < numEntries && currBucketNum < this.numBuckets; i++) {
            final PageID bucketID = HashDirectoryPage.readPageID(page, i);
            final byte depth = HashDirectoryPage.readDepth(page, i);
            
            final String hash = Integer.toString(currBucketNum, 2);
            for (int j = 0; j < this.depth - hash.length(); ++j) {
               sb.append('0');
            }
            sb.append(hash + " (" + depth + "): ");
            if (currBucketNum % (1 << depth) == currBucketNum) {
               final Page<BucketPage> bucketPage = this.bufferManager.pinPage(bucketID);
               final int bucketEntries = BucketPage.computeOnChain(this.bufferManager, bucketPage,
                     BucketPage::getNumEntries, (final Integer a, final Integer b) -> a + b);
               this.bufferManager.unpinPage(bucketPage, UnpinMode.CLEAN);
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
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
         numDirPages++;
      }

      for (int j = 0; j < name.length(); ++j) {
         sb.append('-');
      }

      sb.append("\n");
      sb.append("Total # entries (counted): " + this.numEntries + " (" + totalEntries + ")" + "\n");
      sb.append("Total # buckets (counted, allocated): " + this.numBuckets + " (" + currBucketNum + ", "
            + numBktAlloc + ")" + "\n");
      sb.append("# directory pages: " + numDirPages + "\n");
      sb.append("Global Depth: " + this.depth + "\n");
      return sb;
   }

   @Override
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
   public void printSummary() {
      final StringBuilder sb = new StringBuilder();
      this.prettyPrint(sb);
      System.out.println(sb.toString());
   }

   @Override
   public String toString() {
      return this.fileName.orElse("Temporary Index");
   }

   @Override
   public void close() {
      // TODO Implement this method.
   }

   @Override
   public Iterator<PageID> directoryIterator() {
      // TODO Implement this method.
      return null;
   }

   /**
    * Calculates the maximum number of PageIDs given the writable size.
    * 
    * @param writableSize
    *           writable size
    * @return number of Page IDs
    */
   private static int calcNumEntriesPerPage(final int writableSize) {
      return writableSize / (PageID.SIZE + Byte.BYTES);
   }
}

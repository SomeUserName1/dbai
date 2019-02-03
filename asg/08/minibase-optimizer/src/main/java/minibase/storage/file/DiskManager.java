/*
 * @(#)DiskManager.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.file;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import minibase.Minibase;
import minibase.storage.buffer.PageID;
import minibase.util.Convert;

/**
 * The disk manager is the component of Minibase that takes care of the allocation and deallocation of pages
 * within the database. It also performs reads and writes of pages to and from disk, providing a logical file
 * layer.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 */
public final class DiskManager implements Closeable {

   /** Size of a page, in bytes. */
   public static final int PAGE_SIZE = 1 << 10;

   /** Page number of the first page in a database file. */
   private static final int FIRST_PAGE_NO = 0;

   /** Maximum size of a name (i.e. of files or attributes). */
   public static final int NAME_MAXLEN = 50;

   /** Block number of the first map page. */
   private static final int FIRST_MAP_PAGE = 1;

   /** Number of actual bits per page. */
   private static final int BITS_PER_PAGE = DiskManager.PAGE_SIZE * Byte.SIZE;

   /** Offset of the next page id. */
   private static final int NEXT_PAGE = 0;

   /** Offset of the number of file entries. */
   private static final int NUM_OF_ENTRIES = 4;

   /** Offset of the start of file entries. */
   private static final int START_FILE_ENTRIES = 8;

   /** Size of the internal page buffer. */
   private static final int PAGE_BUFFER_SIZE = 16;

   /** Size of a file entry (in bytes). */
   private static final int SIZE_OF_FILE_ENTRY = 4 + DiskManager.NAME_MAXLEN + 2;

   /** Amount of additional bytes used by directory pages. */
   private static final int DIR_PAGE_USED_BYTES = 8 + 8;

   /** Amount of additional bytes used by the first page. */
   private static final int FIRST_PAGE_USED_BYTES = DiskManager.DIR_PAGE_USED_BYTES + 4;

   /** Offset for the total number of pages. */
   private static final int FIRST_PAGE_NUM_DB_PAGES = DiskManager.PAGE_SIZE - 4;

   /** Actual reference to the Minibase file. */
   private DiskFile diskFile;

   /** Database file size, in pages. */
   private final long numMapPages;

   /** Buffer for header pages. */
   private final byte[][] buffer;

   /** Stores, where the last page was allocated. */
   private int lastAllocPage;

   /** Stores, where the last page was allocated. */
   private int lastAllocByte;

   /**
    * Page numbers of the pages in the buffer, the most significant bit is used as dirty flag.
    */
   private final int[] pageNrs;

   /** Number of pages in the buffer. */
   private int bufferSize;

   /** Number of disk reads since construction. */
   private int readCount;

   /** Number of disk writes since construction. */
   private int writeCount;

   /**
    * Creates a disk manager.
    * @param diskFile disk file
    * @param numMapPages number of bitmap pages
    * @param buffer internal page buffer
    * @param pageNrs page numbers of buffered pages
    * @param bufferSize number of currently buffered internal pages
    */
   private DiskManager(final DiskFile diskFile, final long numMapPages, final byte[][] buffer,
         final int[] pageNrs, final int bufferSize) {
      this.diskFile = diskFile;
      this.numMapPages = numMapPages;
      this.buffer = buffer;
      this.pageNrs = pageNrs;
      this.bufferSize = bufferSize;
   }

   /**
    * Creates a new disk manager.
    *
    * @param diskFile
    *           disk file to store the database in
    * @return the disk manager
    */
   public static DiskManager create(final DiskFile diskFile) {
      final long numPages = diskFile.getNumPages();

      // create and initialize the first DB page
      final byte[][] buffer = new byte[PAGE_BUFFER_SIZE][DiskManager.PAGE_SIZE];
      final int[] pageNrs = new int[PAGE_BUFFER_SIZE];
      Arrays.fill(pageNrs, (byte) -1);
      final byte[] firstPage = buffer[0];
      pageNrs[0] = DiskManager.FIRST_PAGE_NO | 0x80000000;

      // set the next page to invalid
      Convert.writeInt(firstPage, DiskManager.NEXT_PAGE, PageID.INVALID.getValue());

      // set the number entries
      final int numEntries =
            (DiskManager.PAGE_SIZE - DiskManager.FIRST_PAGE_USED_BYTES) / DiskManager.SIZE_OF_FILE_ENTRY;
      Convert.writeInt(firstPage, DiskManager.NUM_OF_ENTRIES, numEntries);

      // initialize the page entries
      for (int entryNo = 0; entryNo < numEntries; entryNo++) {
         final int offset = DiskManager.START_FILE_ENTRIES + entryNo * DiskManager.SIZE_OF_FILE_ENTRY;
         Convert.writeInt(firstPage, offset, PageID.INVALID.getValue());
      }

      Convert.writeInt(firstPage, DiskManager.FIRST_PAGE_NUM_DB_PAGES, (int) numPages);

      // calculate how many pages are needed for the space map; reserve
      // pages 0 and 1 and as many additional pages as are needed
      int bufferSize = 1;
      final long numMapPages = (numPages + BITS_PER_PAGE - 1) / BITS_PER_PAGE;
      long bitsNeeded = numMapPages + 1;
      final byte[] page = new byte[PAGE_SIZE];
      for (int i = 0; bitsNeeded > 0; i++) {
         Arrays.fill(page, (byte) 0);
         // this loop actually flips the bits on the current page
         int bitsThisPage = (int) Math.min(bitsNeeded, DiskManager.BITS_PER_PAGE);
         bitsNeeded -= bitsThisPage;

         for (int j = 0; bitsThisPage > 0; j++) {
            final int bitsThisByte = Math.min(bitsThisPage, Byte.SIZE);
            page[j] = (byte) ((1 << bitsThisByte) - 1);
            bitsThisPage -= bitsThisByte;
         }
         diskFile.writePage(FIRST_MAP_PAGE + i, page);
         if (bufferSize < PAGE_BUFFER_SIZE) {
            System.arraycopy(page, 0, buffer[bufferSize], 0, PAGE_SIZE);
            pageNrs[bufferSize] = FIRST_MAP_PAGE + i;
            bufferSize++;
         }
      }

      return new DiskManager(diskFile, numMapPages, buffer, pageNrs, bufferSize);
   }

   /**
    * Constructor for opening an existing database file.
    *
    * @param diskFile
    *           disk file
    * @return disk manager
    */
   public static DiskManager open(final DiskFile diskFile) {
      final byte[][] buffer = new byte[PAGE_BUFFER_SIZE][DiskManager.PAGE_SIZE];
      final int[] pageNrs = new int[PAGE_BUFFER_SIZE];
      Arrays.fill(pageNrs, (byte) -1);

      // get the total number of pages
      diskFile.readPage(FIRST_PAGE_NO, buffer[0]);
      pageNrs[0] = FIRST_PAGE_NO;
      final long storedNumPages = Convert.readInt(buffer[0], FIRST_PAGE_NUM_DB_PAGES) & 0xFFFFFFFFL;
      final long numPages = diskFile.getNumPages();
      if (storedNumPages != numPages) {
         throw new IllegalStateException("Wrong disk file size: " + storedNumPages + " vs. " + numPages);
      }
      final long numMapPages = (numPages + BITS_PER_PAGE - 1) / BITS_PER_PAGE;
      return new DiskManager(diskFile, numMapPages, buffer, pageNrs, 1);
   }

   /**
    * Returns the database file.
    *
    * @return database file
    */
   public File getDatabaseFile() {
      return this.diskFile.getFileName();
   }

   /**
    * Gets the number of disk reads since construction.
    *
    * @return the read count
    */
   public int getReadCount() {
      return this.readCount;
   }

   /**
    * Gets the number of disk writes since construction.
    *
    * @return the write count
    */
   public int getWriteCount() {
      return this.writeCount;
   }

   /**
    * Gets the number of allocated disk pages.
    *
    * @return the allocation count
    */
   public int getAllocCount() {
      // iterate each page in the space map
      int count = 0;
      for (int i = 0; i < this.numMapPages; i++) {
         final byte[] mapPage = this.getPage(DiskManager.FIRST_MAP_PAGE + i, false);
         for (int j = 0; j < DiskManager.PAGE_SIZE; j += 4) {
            count += Integer.bitCount(Convert.readInt(mapPage, j));
         }
      }
      return count;
   }

   /**
    * Allocates a single page on disk.
    *
    * @return The new page's ID
    * @throws IllegalStateException
    *            if the database is full
    */
   public PageID allocatePage() {
      int byteStart = this.lastAllocByte;

      // simpler loop for a single page
      for (int p = this.lastAllocPage; p < this.numMapPages; p++) {
         final int mapPageNo = DiskManager.FIRST_MAP_PAGE + p;
         final byte[] data = this.getPage(mapPageNo, false);
         for (int b = byteStart; b < data.length; b++) {
            final int inv = ~data[b];
            if (inv != 0) {
               // there is a free spot
               final int freeBit = Integer.numberOfTrailingZeros(inv);
               final int pageNo = p * DiskManager.BITS_PER_PAGE + b * Byte.SIZE + freeBit;
               if (pageNo >= this.diskFile.getNumPages()) {
                  // must be the last page
                  break;
               }

               // remember where the search stopped, for the next allocation
               this.lastAllocPage = p;
               this.lastAllocByte = b;

               // mark page as allocated and return page ID
               data[b] |= 1 << freeBit;
               this.markPageDirty();
               return PageID.getInstance(pageNo);
            }
         }

         // reset byte start, to search from first byte on
         byteStart = 0;
      }
      throw new IllegalStateException("Database is full; allocation aborted.");
   }

   /**
    * Allocates a set of pages on disk, given the run size.
    *
    * @param runSize
    *           number of pages to allocate
    * @return The new page's id
    * @throws IllegalArgumentException
    *            if runSize is invalid
    * @throws IllegalStateException
    *            if the database is full
    */
   public PageID allocatePages(final int runSize) {
      // validate the run size
      if (runSize < 1 || runSize > this.diskFile.getNumPages()) {
         throw new IllegalArgumentException(
               "Invalid run size (" + runSize + "/" + this.diskFile.getNumPages() + "); allocate aborted");
      }

      // calculate the run in the space map
      int currentRunStart = 0;
      int currentRunLength = 0;

      // this loop goes over each page in the space map
      for (int i = this.lastAllocPage; i < this.numMapPages; i++) {
         // pin the space-map page
         final int mapPageNo = i + 1;
         final byte[] data = this.getPage(mapPageNo, false);

         // get the number of bits on current page
         int numBitsThisPage = (int) Math.min(this.diskFile.getNumPages() - i * BITS_PER_PAGE,
               DiskManager.BITS_PER_PAGE);

         // Walk the page looking for a sequence of 0 bits of the appropriate length.
         // The outer loop steps through the page's bytes, the inner one steps through each
         // byte's bits.
         for (int j = 0; numBitsThisPage > 0 && currentRunLength < runSize; j++) {

            // search the page for an empty run
            for (int bit = 0; bit < 8 && numBitsThisPage > 0 && currentRunLength < runSize; bit++) {
               // if a non-empty page is found
               if ((data[j] & 1 << bit) != 0) {
                  currentRunStart += currentRunLength + 1;
                  currentRunLength = 0;
               } else {
                  currentRunLength++;
               }

               // advance to the next page
               numBitsThisPage--;
            }
         }
      }

      // check for disk full exception
      if (currentRunLength < runSize) {
         throw new IllegalStateException("Not enough space left; allocate aborted");
      }

      // update the space map and return the resulting page id
      this.setBits(currentRunStart, runSize, true);
      return PageID.getInstance(currentRunStart);
   }

   /**
    * Deallocates a single page (i.e. run size 1) on disk.
    *
    * @param pageID
    *           identifies the page to deallocate
    * @throws IllegalArgumentException
    *            if pageID is invalid
    */
   public void deallocatePage(final PageID pageID) {
      this.deallocatePages(pageID, 1);
   }

   /**
    * Deallocates a set of pages on disk, given the run size.
    *
    * @param firstID
    *           identifies the first page to deallocate
    * @param runSize
    *           number of pages to deallocate
    * @throws IllegalArgumentException
    *            if firstID or runSize is invalid
    */
   public void deallocatePages(final PageID firstID, final int runSize) {
      // validate the run size
      if (runSize < 1) {
         throw new IllegalArgumentException("Invalid run size; deallocate aborted");
      }

      // update the space map
      this.setBits(this.validatePageID(firstID, "deallocate"), runSize, false);

      // check if an earlier spot in the file is free now
      final int mapPage = firstID.getValue() / BITS_PER_PAGE;
      final int mapByte = (firstID.getValue() % BITS_PER_PAGE) / Byte.SIZE;
      if (this.lastAllocPage > mapPage
            || (this.lastAllocPage == mapPage && this.lastAllocByte > mapByte)) {
         this.lastAllocPage = mapPage;
         this.lastAllocByte = mapByte;
      }
   }

   /**
    * Reads the contents of the specified page from disk.
    *
    * @param pageID
    *           identifies the page to read
    * @param data
    *           output param to hold the contents of the page
    * @throws IllegalArgumentException
    *            if pageID is invalid
    */
   public void readPage(final PageID pageID, final byte[] data) {
      // seek to the correct page on disk and read it
      final int pageNo = this.validatePageID(pageID, "read");
      if (!this.isAllocated(pageNo)) {
         throw new IllegalArgumentException("Page with ID " + pageID + " is not allocated.");
      }
      this.diskFile.readPage(pageNo, data);
      this.readCount++;
   }

   /**
    * Writes the contents of the given page to disk.
    *
    * @param pageID
    *           identifies the page to write
    * @param data
    *           holds the contents of the page
    * @throws IllegalArgumentException
    *            if pageno is invalid
    */
   public void writePage(final PageID pageID, final byte[] data) {
      final int pageNo = this.validatePageID(pageID, "write");
      if (!this.isAllocated(pageNo)) {
         throw new IllegalArgumentException("Page with ID " + pageID + " is not allocated.");
      }
      this.diskFile.writePage(pageNo, data);
      this.writeCount++;
   }

   /**
    * Flushes all buffered pages to disk.
    */
   public void flushAllPages() {
      for (int i = 0; i < this.buffer.length; i++) {
         if (this.pageNrs[i] < -1) {
            this.pageNrs[i] &= 0x7FFFFFFF;
            this.diskFile.writePage(this.pageNrs[i], this.buffer[i]);
         }
      }
   }

   /**
    * Adds a file entry to the header page(s); each entry contains the name of the file and the PageId of the
    * file's first page.
    *
    * @param fileName
    *           file name
    * @param startPageID
    *           page ID of the file's first page
    * @throws IllegalArgumentException
    *            if fileName or startPageID is invalid
    */
   public void addFileEntry(final String fileName, final PageID startPageID) {

      // validate the arguments
      if (fileName.length() > DiskManager.NAME_MAXLEN) {
         throw new IllegalArgumentException("Filename too long; add entry aborted");
      }

      if (startPageID.getValue() < 0 || startPageID.getValue() >= this.diskFile.getNumPages()) {
         throw new IllegalArgumentException("Invalid page number; add entry aborted");
      }

      // does the file already exist?
      if (this.getFileEntry(fileName) != null) {
         throw new IllegalArgumentException("File entry already exists; add entry aborted");
      }

      // search the header pages for the entry slot
      int nextEntryNo = DiskManager.FIRST_PAGE_NO;
      while (true) {
         // pin the next header page and get its next
         final int headerID = nextEntryNo;
         nextEntryNo = this.getNextPage(headerID);

         // search the header page for an empty entry
         final int numEntries = this.getNumEntries(headerID);
         for (int entry = 0; entry < numEntries; entry++) {
            if (this.getFileEntryPageNo(headerID, entry) < 0) {
               // found an empty slot, fill it
               this.setFileEntry(headerID, entry, fileName, startPageID);
               return;
            }
         }

         if (nextEntryNo == PageID.INVALID.getValue()) {
            // allocate the new header page
            final int newHeaderNo = this.allocatePage().getValue();

            // set the next-page pointer on the previous directory page
            Convert.writeInt(this.getPage(headerID, true), DiskManager.NEXT_PAGE, newHeaderNo);

            // pin the newly-allocated directory page
            this.newHeaderPage(newHeaderNo, false);
            this.setFileEntry(newHeaderNo, 0, fileName, startPageID);
            return;
         }
      }
   }

   /**
    * Deletes a file entry from the header page(s).
    *
    * @param fileName
    *           file name
    * @throws IllegalArgumentException
    *            if fileName is invalid
    */
   public void deleteFileEntry(final String fileName) {
      // search the header pages for the entry slot
      int nextHeaderNo = 0;
      do {
         // pin the next header page and get its next
         final int headerNo = nextHeaderNo;
         nextHeaderNo = this.getNextPage(headerNo);

         // search the header page for an the entry
         final int numEntries = this.getNumEntries(headerNo);
         for (int entryNo = 0; entryNo < numEntries; entryNo++) {
            if (this.getFileEntryPageNo(headerNo, entryNo) >= 0
                  && this.getFileEntryName(headerNo, entryNo).equalsIgnoreCase(fileName)) {
               // delete record
               this.setFileEntry(headerNo, entryNo, "\0", PageID.INVALID);
               return;
            }
         }
      } while (nextHeaderNo >= 0);

      throw new IllegalArgumentException("File entry not found; delete entry aborted");
   }

   /**
    * Looks up the entry for the given file name.
    *
    * @param fileName
    *           file name
    * @return PageID of the file's first page, or {@code null} if the file doesn't exist
    */
   public PageID getFileEntry(final String fileName) {
      // search the header pages for the entry slot
      int nextHeaderNo = 0;
      do {
         // search the header page for the entry
         final int headerNo = nextHeaderNo;
         final int numEntries = this.getNumEntries(headerNo);
         for (int entryNo = 0; entryNo < numEntries; entryNo++) {
            final int pageNo = this.getFileEntryPageNo(headerNo, entryNo);
            if (pageNo >= 0 && this.getFileEntryName(headerNo, entryNo).equalsIgnoreCase(fileName)) {
               // entry found
               return PageID.getInstance(pageNo);
            }
         }
         nextHeaderNo = this.getNextPage(headerNo);
      } while (nextHeaderNo >= 0);

      // return null if not found
      return null;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      int bitNumber = 0;

      // this loop goes over each page in the space map
      sb.append("num_map_pages = ").append(this.numMapPages)
            .append("\nnum_pages = ").append(this.diskFile.getNumPages());
      for (int i = 0; i < this.numMapPages; i++) {
         // pin the space-map page
         // space map starts at page1
         final byte[] data = this.getPage(i + 1, false);

         // how many bits should we examine on this page?
         int numBitsThisPage = (int) Math.min(this.diskFile.getNumPages() - i * DiskManager.BITS_PER_PAGE,
               DiskManager.BITS_PER_PAGE);
         sb.append("\n\nnum_bits_this_page = ").append(numBitsThisPage).append("\n\n");
         if (i > 0) {
            sb.append('\t');
         }

         // The outer loop steps through the page's bytes, the inner one steps through each
         // byte's bits.
         for (int pgptr = 0; numBitsThisPage > 0; pgptr++) {
            for (int pos = 0; pos < 8 && numBitsThisPage > 0; pos++) {
               if (bitNumber % 10 == 0) {
                  if (bitNumber % 50 == 0) {
                     if (bitNumber > 0) {
                        sb.append("\n");
                     }
                     sb.append('\t').append(bitNumber).append(": ");
                  } else {
                     sb.append(' ');
                  }
               }

               sb.append((data[pgptr] & 1 << pos) != 0 ? "1" : "0");
               if (--numBitsThisPage == 0) {
                  break;
               }
               bitNumber++;
            }
         }
      }
      return sb.toString();
   }

   @Override
   public void close() {
      if (this.diskFile != null) {
         try {
            this.flushAllPages();
            this.diskFile.close();
            this.diskFile = null;
         } catch (final IOException exc) {
            Minibase.haltSystem(exc);
         }
      }
   }

   /**
    * Destroys the database, removing the file that stores it.
    *
    * @throws IOException if the file cannot be deleted
    */
   public void destroy() throws IOException {
      final File file = this.diskFile.getFileName();
      this.close();
      Files.delete(file.toPath());
   }

   /**
    * Checks in the map pages if the page with the given page number is allocated.
    * 
    * @param pageNo
    *           page number to look up
    * @return {@code true} if the page is allocated, {@code false} otherwise
    */
   private boolean isAllocated(final int pageNo) {
      final int mapPageNo = DiskManager.FIRST_MAP_PAGE + pageNo / DiskManager.BITS_PER_PAGE;
      final byte[] mapPage = this.getPage(mapPageNo, false);
      final int bitInPage = pageNo % DiskManager.BITS_PER_PAGE;
      final int byt = mapPage[bitInPage / Byte.SIZE];
      final int bitPos = bitInPage % Byte.SIZE;
      return ((byt >>> bitPos) & 1) != 0;
   }

   /**
    * Sets 'runSize' bits in the space map to the given value, starting from 'startPageNo'.
    *
    * @param startPageNo
    *           number of the first bit to set
    * @param runSize
    *           number of bits to set
    * @param bit
    *           value of the bits
    */
   private void setBits(final int startPageNo, final long runSize, final boolean bit) {
      final int firstMapPage = DiskManager.FIRST_MAP_PAGE + startPageNo / DiskManager.BITS_PER_PAGE;
      final int lastMapPage = (int) (DiskManager.FIRST_MAP_PAGE
            + (startPageNo + runSize - 1) / DiskManager.BITS_PER_PAGE);
      final int numPages = lastMapPage - firstMapPage + 1;

      long bitsNeeded = runSize;
      int firstBitThisPage = startPageNo % DiskManager.BITS_PER_PAGE;
      for (int i = 0; i < numPages; i++) {
         int bitsThisPage = (int) Math.min(bitsNeeded, DiskManager.BITS_PER_PAGE - firstBitThisPage);
         bitsNeeded -= bitsThisPage;

         // this loop actually flips the bits on the current page
         final byte[] data = this.getPage(firstMapPage + i, true);
         final int firstByteNo = firstBitThisPage / Byte.SIZE;
         final int lastByteNo = (firstBitThisPage + bitsThisPage - 1) / Byte.SIZE;
         final int numBytes = lastByteNo - firstByteNo + 1;

         int firstBitThisByte = firstBitThisPage % Byte.SIZE;
         for (int j = 0; j < numBytes; j++) {
            final int bitsThisByte = Math.min(bitsThisPage, Byte.SIZE - firstBitThisByte);
            final int mask = (1 << bitsThisByte) - 1 << firstBitThisByte;
            if (bit) {
               data[firstByteNo + j] |= mask;
            } else {
               data[firstByteNo + j] &= ~mask;
            }
            bitsThisPage -= bitsThisByte;
            firstBitThisByte = 0;
         }
         firstBitThisPage = 0;
      }
   }

   /**
    * Checks if the page ID is in the correct range.
    *
    * @param pageID
    *           ID to check
    * @param desc
    *           description of the checking operation for the error message
    * @return the page number
    */
   private int validatePageID(final PageID pageID, final String desc) {
      // validate the page id
      if (pageID.getValue() < DiskManager.FIRST_MAP_PAGE + this.numMapPages
            || pageID.getValue() >= this.diskFile.getNumPages()) {
         throw new IllegalArgumentException("Invalid page number " + pageID + "; " + desc + " aborted");
      }
      return pageID.getValue();
   }

   /**
    * Initializes the header page with default values.
    *
    * @param pageNo
    *           the page number
    * @param firstPage
    *           flag for the first page of the database
    * @return the page
    */
   private byte[] newHeaderPage(final int pageNo, final boolean firstPage) {
      // zero-fill the page
      final byte[] page = this.getPage(pageNo, true);
      Arrays.fill(page, (byte) 0);

      // set the next page to invalid
      Convert.writeInt(page, DiskManager.NEXT_PAGE, PageID.INVALID.getValue());

      // set the number entries
      final int pageUsedBytes = firstPage ? DiskManager.FIRST_PAGE_USED_BYTES
            : DiskManager.DIR_PAGE_USED_BYTES;
      final int numEntries = (DiskManager.PAGE_SIZE - pageUsedBytes) / DiskManager.SIZE_OF_FILE_ENTRY;
      Convert.writeInt(page, DiskManager.NUM_OF_ENTRIES, numEntries);

      // initialize the page entries
      for (int entryNo = 0; entryNo < numEntries; entryNo++) {
         final int offset = DiskManager.START_FILE_ENTRIES + entryNo * DiskManager.SIZE_OF_FILE_ENTRY;
         Convert.writeInt(page, offset, PageID.INVALID.getValue());
      }

      return page;
   }

   /**
    * Reads the page number of the next header page from the given one.
    *
    * @param pageNo
    *           page number of the current page
    * @return next page's page number
    */
   private int getNextPage(final int pageNo) {
      return Convert.readInt(this.getPage(pageNo, false), DiskManager.NEXT_PAGE);
   }

   /**
    * Reads the number of entries of the header page with the given page number.
    *
    * @param pageNo
    *           page number
    * @return number of entries
    */
   private int getNumEntries(final int pageNo) {
      return Convert.readInt(this.getPage(pageNo, false), DiskManager.NUM_OF_ENTRIES);
   }

   /**
    * Gets the name of the file entry at the given offset of the page with the given page number.
    *
    * @param pageNo
    *           header page number
    * @param entryNo
    *           entry number
    * @return the entry's name
    */
   private String getFileEntryName(final int pageNo, final int entryNo) {
      final int position = DiskManager.START_FILE_ENTRIES + entryNo * DiskManager.SIZE_OF_FILE_ENTRY;
      final byte[] page = this.getPage(pageNo, false);
      return Convert.readString(page, position + 4, DiskManager.NAME_MAXLEN + 2);
   }

   /**
    * Gets the page number of the first page of the file entry at the given offset of the page with the given
    * page number.
    *
    * @param pageNo
    *           header page number
    * @param entryNo
    *           entry number
    * @return the entry's first page' number
    */
   private int getFileEntryPageNo(final int pageNo, final int entryNo) {
      final int position = DiskManager.START_FILE_ENTRIES + entryNo * DiskManager.SIZE_OF_FILE_ENTRY;
      final byte[] page = this.getPage(pageNo, false);
      return Convert.readInt(page, position);
   }

   /**
    * Sets the file entry at the given offset of the page with the given page number.
    *
    * @param pageNo
    *           header page number
    * @param entryNo
    *           number of the entry to set
    * @param fname
    *           file name
    * @param pageID
    *           first page's page ID
    */
   private void setFileEntry(final int pageNo, final int entryNo, final String fname, final PageID pageID) {
      final int position = DiskManager.START_FILE_ENTRIES + entryNo * DiskManager.SIZE_OF_FILE_ENTRY;
      final byte[] page = this.getPage(pageNo, true);
      Convert.writeInt(page, position, pageID.getValue());
      Convert.writeString(page, position + 4, fname, DiskManager.NAME_MAXLEN + 2);
   }

   /**
    * Gets the header page with the given page number. A fixed number of pages is cached in a LRU cache.
    *
    * @param pageNo
    *           page number of the header page to get
    * @param write
    *           write flag, marks the page as dirty
    * @return the page contents
    */
   private byte[] getPage(final int pageNo, final boolean write) {
      final int mask = write ? 0x80000000 : 0x00000000;
      final int numBuffers = this.buffer.length;
      for (int i = 0; i < this.bufferSize; i++) {
         final int currNo = this.pageNrs[i];
         if ((currNo & 0x7FFFFFFF) == pageNo) {
            final byte[] buff = this.buffer[i];
            System.arraycopy(this.pageNrs, 0, this.pageNrs, 1, i);
            System.arraycopy(this.buffer, 0, this.buffer, 1, i);
            this.pageNrs[0] = currNo | mask;
            this.buffer[0] = buff;
            return buff;
         }
      }

      if (this.bufferSize < this.buffer.length) {
         // there are empty buffers left
         final int pos = this.bufferSize++;
         final byte[] buff = this.buffer[pos];
         System.arraycopy(this.pageNrs, 0, this.pageNrs, 1, pos);
         System.arraycopy(this.buffer, 0, this.buffer, 1, pos);
         this.pageNrs[0] = pageNo | mask;
         this.buffer[0] = buff;
         this.diskFile.readPage(pageNo, buff);
         return buff;
      }

      // all buffer pages are full, evict the last one
      final int last = numBuffers - 1;
      final byte[] buff = this.buffer[last];
      if (this.pageNrs[last] < 0) {
         // the page is dirty, flush it to disk
         this.diskFile.writePage(this.pageNrs[last] & 0x7FFFFFFF, this.buffer[last]);
      }
      // shift all entries to the right
      System.arraycopy(this.pageNrs, 0, this.pageNrs, 1, this.bufferSize - 1);
      System.arraycopy(this.buffer, 0, this.buffer, 1, this.bufferSize - 1);

      // read the page into the newly freed buffer
      this.pageNrs[0] = pageNo | mask;
      this.buffer[0] = buff;
      this.diskFile.readPage(pageNo, buff);
      return buff;
   }

   /**
    * Marks the last accessed header page as dirty.
    */
   private void markPageDirty() {
      this.pageNrs[0] |= 0x80000000;
   }
}

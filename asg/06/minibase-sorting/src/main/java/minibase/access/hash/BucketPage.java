/*
 * @(#)BucketPage.java   1.0   Sep 17, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.access.index.IndexEntry;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.PageType;
import minibase.storage.file.DiskManager;

/**
 * The bucket page of a linear hash index. The page stores entries at the beginning of the byte array and
 * meta-data at the end.
 *
 * <pre>
 * ┌──────────┐
 * │BucketPage│
 * ├──────────┴──────────────────────────────────────┬─────────────┬─────────────────┐
 * │                                                 │             │                 │
 * │                     data[]                      │ NUM_ENTRIES │OVERFLOW_POINTER │
 * │                                                 │             │                 │
 * └───────────────────────▲─────────────────────────┴──────▲──────┴────────▲────────┘
 *                         │                                │               │
 *                         │                                │               │
 *                         │                                │               │
 *                  MAX_ENTRY_SIZE                    Integer.BYTES    PageID.SIZE
 * </pre>
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 * @author Manfred Schaefer &lt;manfred.schaefer@uni-konstanz.de&gt;
 */
public final class BucketPage implements PageType {

   /** Offset of pointer to the next directory page. */
   private static final int OVERFLOW_POINTER_OFFSET = DiskManager.PAGE_SIZE - PageID.BYTES;
   /** Offset of number of entries on the page. */
   private static final int OFFSET_NUM_ENTRIES = OVERFLOW_POINTER_OFFSET - Integer.BYTES;
   /** Maximum size of an entry in the bucket page. */
   public static final int MAX_ENTRY_SIZE = OFFSET_NUM_ENTRIES;

   /** Hidden default constructor. */
   private BucketPage() {
      throw new AssertionError();
   }

   /**
    * Allocates a new and pinned bucket page.
    *
    * @param bufferManager
    *           the buffer manager
    * @return a freshly allocated and pinned bucket page
    */
   public static Page<BucketPage> newPage(final BufferManager bufferManager) {
      @SuppressWarnings("unchecked")
      final Page<BucketPage> page = (Page<BucketPage>) bufferManager.newPage();
      page.writePageID(OVERFLOW_POINTER_OFFSET, PageID.INVALID);
      return page;
   }

   /**
    * Sets the overflow pointer.
    *
    * @param page
    *           the current page
    * @param next
    *           the next bucket page
    */
   public static void setOverflowPointer(final Page<BucketPage> page, final PageID next) {
      page.writePageID(BucketPage.OVERFLOW_POINTER_OFFSET, next);
   }

   /**
    * Gets the next overflow page id.
    *
    * @param page
    *           the current page
    * @return the page id of the next overflow page in the chain
    */
   public static PageID getNextPagePointer(final Page<BucketPage> page) {
      return page.readPageID(BucketPage.OVERFLOW_POINTER_OFFSET);
   }

   /**
    * Gets the number of entries stored on the given {@link BucketPage}.
    *
    * @param page
    *           the bucket page
    * @return the number of entries that are currently stored on the given bucket page
    */
   public static int getNumEntries(final Page<BucketPage> page) {
      return page.readInt(OFFSET_NUM_ENTRIES);
   }

   /**
    * Sets the number of entries stored on the given {@link BucketPage}.
    *
    * @param page
    *           the bucket page
    * @param numEntries
    *           the number of entries to set
    */
   static void setNumEntries(final Page<BucketPage> page, final int numEntries) {
      page.writeInt(OFFSET_NUM_ENTRIES, numEntries);
   }

   /**
    * Appends the given data entry to the bucket page.
    *
    * @param page
    *           page to append to
    * @param entry
    *           data entry to append
    * @param entryLength
    *           length of a fixed-size entry stored on the page
    */
   public static void appendDataEntry(final Page<BucketPage> page, final IndexEntry entry, final int entryLength) {
      final int numEntries = BucketPage.getNumEntries(page);
      entry.writeData(page.getData(), numEntries * entryLength);
      BucketPage.setNumEntries(page, numEntries + 1);
   }

   /**
    * Reads data entry.
    *
    * @param page
    *           the page to read from
    * @param pos
    *           position of the data entry
    * @param type
    *           the type of the search keys
    * @return the data entry at the specified position, or {@code null} if there was no valid entry at the
    *         space
    */
   public static IndexEntry readDataEntryAt(final Page<BucketPage> page, final int pos, final SearchKeyType type) {
      if (pos < 0) {
         throw new IllegalArgumentException("Position must not be negative.");
      }
      final int offset = pos * (type.getKeyLength() + RecordID.BYTES);
      return IndexEntry.fromBuffer(page.getData(), offset, type);
   }

   /**
    * Reads the search key of the entry at the given position inside the given page.
    * @param page page to read from
    * @param pos index of the entry inside the page
    * @param type the type of the search keys
    * @return the search key that was read
    */
   public static SearchKey readSearchKey(final Page<BucketPage> page, final int pos, final SearchKeyType type) {
      if (pos < 0) {
         throw new IllegalArgumentException("Position must not be negative.");
      }
      return IndexEntry.readKey(page.getData(), pos * (type.getKeyLength() + RecordID.BYTES), type);
   }

   /**
    * Removes the data entry at the given position. The operation keeps the page tightly packed, so the
    * position at which an entry was removed is not necessarily invalidated and might still contain a valid
    * entry.
    *
    * @param page
    *           the page
    * @param pos
    *           the position to remove at
    * @param type
    *           the type of the search keys
    * @return {@code true} if the page is empty after the deletion, {@code false} otherwise
    */
   public static boolean removeDataEntryAt(final Page<BucketPage> page, final int pos, final SearchKeyType type) {
      final int entriesLeft = BucketPage.getNumEntries(page) - 1;
      if (pos < 0 || pos > entriesLeft) {
         throw new IllegalArgumentException(
               "Specified position " + pos + " is outside of the range of valid entries.");
      }

      BucketPage.setNumEntries(page, entriesLeft);
      if (entriesLeft > 0) {
         // move last entry to open spot to keep the page tightly packed, last spot is now invalidated
         BucketPage.moveEntry(page, entriesLeft, pos, type);
         return false;
      }
      return true;
   }

   /**
    * Checks whether the given page has space for the given entry left.
    *
    * @param page
    *           page to check
    * @param entryLength
    *           length of a fixed-size entry stored on the page
    * @return {@code true} if the entry fits on the page, {@code false} otherwise
    */
   public static boolean hasSpaceLeft(final Page<BucketPage> page, final int entryLength) {
      final int num = BucketPage.getNumEntries(page);
      final int used = num * entryLength;
      return entryLength <= BucketPage.OFFSET_NUM_ENTRIES - used;
   }

   /**
    * Gets the maximum number of entries that can be stored on a bucket page, given the size of fixed-length
    * entries.
    *
    * @param entrySize
    *           the size of an entry
    * @return the maximum number of entries
    */
   public static int maxNumEntries(final int entrySize) {
      return OFFSET_NUM_ENTRIES / entrySize;
   }

   /**
    * Moves the last entry on the source page to the specified position on the destination page. This is
    * useful for compacting pages after random-access deletions. This function increments the entry counter
    * on the destination page if the destination position is equal to the destination count (entry was appended).
    *
    * @param source
    *           the source page to take the last entry from
    * @param dest
    *           the destination page
    * @param pos
    *           the position on the destination page to put the entry into
    * @param type
    *           the type of the search keys
    * @return {@code true} if the source page is now empty, {@code false} otherwise
    */
   public static boolean moveLastEntryTo(final Page<BucketPage> source, final Page<BucketPage> dest,
         final int pos, final SearchKeyType type) {
      final int last = BucketPage.getNumEntries(source) - 1;

      BucketPage.moveEntry(source, last, dest, pos, type);

      BucketPage.setNumEntries(source, last);
      final int destNumEntries = BucketPage.getNumEntries(dest);
      if (destNumEntries == pos) {
         BucketPage.setNumEntries(dest, destNumEntries + 1);
      }

      return last == 0;
   }

   /**
    * Merges the given source page into the given destination page and returns wheter the source page was
    * reused.
    *
    * @param source
    *           page to merge into destination page
    * @param destination
    *           page to merge source page into
    * @param entrySize
    *           size of an entry
    * @return {@code true} if the source page was reused and is the first page in the chain-pair,
    *         {@code false} if the destination page is the only page containing data
    */
   public static boolean merge(final Page<BucketPage> source, final Page<BucketPage> destination,
         final int entrySize) {
      final int sourceEntries = BucketPage.getNumEntries(source);
      final int targetEntries = BucketPage.getNumEntries(destination);
      final int allEntries = sourceEntries + targetEntries;
      final int maxNumEntries = BucketPage.maxNumEntries(entrySize);

      if (allEntries <= maxNumEntries) {
         // we have enough space and can just copy the data buffer
         System.arraycopy(source.getData(), 0, destination.getData(), targetEntries * entrySize,
               sourceEntries * entrySize);
         BucketPage.setNumEntries(destination, allEntries);
         return false;
      }

      // we have to reuse the source page
      final int move = maxNumEntries - targetEntries;
      final int remaining = sourceEntries - move;
      System.arraycopy(source.getData(), remaining * entrySize, destination.getData(),
            targetEntries * entrySize, move * entrySize);
      BucketPage.setNumEntries(source, remaining);
      BucketPage.setNumEntries(destination, maxNumEntries);
      BucketPage.setOverflowPointer(source, destination.getPageID());

      return true;
   }

   /**
    * Copies the data entry at the specified source position to the destination position.
    * If you need to move an entry between two pages, consider using the method
    * {@link BucketPage#moveEntry(Page, int, Page, int, int)}.
    *
    * @param page
    *           page to operate on
    * @param srcPos
    *           position of the entry to move
    * @param destPos
    *           destination of the entry
    * @param type
    *           the type of the search keys
    */
   private static void moveEntry(final Page<BucketPage> page, final int srcPos, final int destPos,
         final SearchKeyType type) {
      BucketPage.moveEntry(page, srcPos, page, destPos, type);
   }

   /**
    * Moves the entry at the specified position on the source page to the specified position on the
    * destination page. Note: you have to manually update the number of entries on each page.
    *
    * @param source
    *           source page
    * @param sourcePos
    *           source position of the entry
    * @param dest
    *           destination page
    * @param destPos
    *           destination position of the entry
    * @param type
    *           the type of the search keys
    */
   static void moveEntry(final Page<BucketPage> source, final int sourcePos,
         final Page<BucketPage> dest, final int destPos, final SearchKeyType type) {
      final int entrySize = type.getKeyLength() + RecordID.BYTES;
      System.arraycopy(source.getData(), sourcePos * entrySize, dest.getData(), destPos * entrySize,
            entrySize);
   }

   /**
    * Overwrites the contents of the second arguments with the contents of the first.
    * @param source source page
    * @param destination target page
    */
   public static void copyContents(final Page<BucketPage> source, final Page<BucketPage> destination) {
      System.arraycopy(source.getData(), 0, destination.getData(), 0, DiskManager.PAGE_SIZE);
   }
}

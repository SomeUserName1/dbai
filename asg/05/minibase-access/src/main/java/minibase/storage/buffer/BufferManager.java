/*
 * @(#)BufferManager.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import minibase.storage.buffer.policy.ReplacementPolicy;
import minibase.storage.buffer.policy.ReplacementPolicy.PageState;
import minibase.storage.file.DiskManager;

/**
 * The buffer manager manages the buffer pool, which consists of a collection of slots (so-called frames) that
 * can hold pages, which have been read into memory. Internally, this buffer pool is represented as an array
 * of page objects. The buffer manager reads disk pages into a main memory page as needed and evicts pages
 * from memory that are no longer required, if the buffer pool has no empty slots to hold new pages. The
 * buffer manager provides the following services.
 * <ul>
 * <li>Pinning and unpinning disk pages to and from frames.</li>
 * <li>Allocating and deallocating sequences of disk pages and coordinating this with the buffer pool.</li>
 * <li>Reading data from disk into memory pages.</li>
 * <li>Flushing pages from the buffer pool to disk.</li>
 * </ul>
 * The buffer manager is used by access methods, heap files, and relational operators to read, write,
 * allocate, and deallocate pages.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class BufferManager {

   /** Actual pool of pages. */
   private final Page< ? >[] bufferPool;

   /** Bitmap containing all currently free pages. */
   private final BitSet freePages;

   /** Maps current page numbers to frames; used for efficient lookups. */
   private final HashMap<PageID, Page< ? >> pageMap;

   /** The replacement policy to use. */
   private final ReplacementPolicy replacementPolicy;

   /** Reference to the disk manager. */
   private final DiskManager diskManager;

   /**
    * Constructs a buffer manager with the given settings.
    *
    * @param diskManager
    *           reference to disk manager
    * @param bufferPoolSize
    *           number of buffers in the buffer pool
    * @param replacementStrategy
    *           replacement strategy used by this buffer manager
    */
   public BufferManager(final DiskManager diskManager, final int bufferPoolSize,
         final ReplacementStrategy replacementStrategy) {
      this.diskManager = diskManager;
      // initialize the buffer pool and frame table
      this.bufferPool = new Page[bufferPoolSize];
      this.freePages = new BitSet(bufferPoolSize);
      for (int i = 0; i < bufferPoolSize; i++) {
         this.bufferPool[i] = new Page<>(i);
         this.freePages.set(i);
      }
      // initialize the specialized page map and replacer
      this.pageMap = new HashMap<>(bufferPoolSize);
      this.replacementPolicy = replacementStrategy.newInstance(bufferPoolSize);
   }

   /**
    * Returns the disk manager used by this buffer manager.
    *
    * @return the underlying disk manager
    */
   public DiskManager getDiskManager() {
      return this.diskManager;
   }

   /**
    * Allocates a new page, pins it and fills the page contents with zeroes.
    *
    * @return the pinned page
    * @throws IllegalStateException
    *            if all pages are pinned (i.e. pool exceeded)
    */
   public Page< ? > newPage() {
      final PageID pageID = this.diskManager.allocatePage();
      final Page< ? > page = this.pinPage(pageID, false);
      Arrays.fill(page.getData(), (byte) 0);
      return page;
   }

   /**
    * Deallocates a single page from disk, freeing it from the pool, if needed. The page must be pinned
    * exactly once.
    *
    * @param page
    *           the page to free
    * @throws IllegalArgumentException
    *            if the page is pinned more of less than one time
    */
   public void freePage(final Page< ? > page) {
      // first check the page state
      if (page.getPinCount() < 1) {
         throw new IllegalArgumentException("Page is not pinned.");
      } else if (page.getPinCount() > 1) {
         throw new IllegalArgumentException("Page " + page.getPageID() + " is pinned more than once.");
      }
      // remove the page from the buffer pool
      this.pageMap.remove(page.getPageID());
      // deallocate the page from disk
      this.diskManager.deallocatePage(page.getPageID());
      page.reset(PageID.INVALID);
      this.freePages.set(page.getIndex());
      // notify the replacer
      this.replacementPolicy.stateChanged(page.getIndex(), PageState.FREE);
   }

   /**
    * Pins a disk page into the buffer pool. If the page is already pinned, this simply increments the pin
    * count. Otherwise, this method selects an empty slot (frame) and reads the page from disk into the buffer
    * pool. If there are no empty slots, the replacement policy of the buffer manager is used to select a
    * victim page. The victim page is then evicted from the buffer pool, flushing it to disk if it is dirty
    * and the new page is loaded into its frame. Finally, the buffer containing the new page is returned.
    *
    * @param pageID
    *           identifies the page to pin
    * @return buffer holding the page contents
    * @param <T>
    *           type of the pinned page (not checked)
    */
   public <T extends PageType> Page<T> pinPage(final PageID pageID) {
      return this.pinPage(pageID, true);
   }

   /**
    * Pins a disk page into the buffer pool. If the page is already pinned, this simply increments the pin
    * count. Otherwise, this method selects an empty slot (frame) and reads the page from disk into the buffer
    * pool. If there are no empty slots, the replacement policy of the buffer manager is used to select a
    * victim page. The victim page is then evicted from the buffer pool, flushing it to disk if it is dirty.
    * If requested, the new page contents are then read from disk. Finally, the buffer containing the new page
    * is returned.
    *
    * @param pageID
    *           identifies the page to pin
    * @param readFromDisk
    *           if the contents should be read from disk
    * @return buffer holding the page contents
    * @param <T>
    *           type of the pinned page (not checked)
    */
   @SuppressWarnings("unchecked")
   <T extends PageType> Page<T> pinPage(final PageID pageID, final boolean readFromDisk) {
      // first check if the page is already pinned
      if (this.pageMap.containsKey(pageID)) {
         final Page<T> page = (Page<T>) this.pageMap.get(pageID);

         // increment the pin count, notify the replacer, and return the buffer
         page.incrementPinCount();
         if (page.getPinCount() == 1) {
            this.replacementPolicy.stateChanged(page.getIndex(), PageState.PINNED);
         }
         return page;
      }

      final Page<T> page;
      if (!this.freePages.isEmpty()) {
         // take the free page first
         final int index = this.freePages.nextSetBit(0);
         this.freePages.clear(index);
         page = (Page<T>) this.bufferPool[index];
      } else {
         // select an available frame
         final int victim = this.replacementPolicy.pickVictim();
         if (victim < 0) {
            throw new IllegalStateException("Buffer pool exceeded");
         }
         page = (Page<T>) this.bufferPool[victim];

         // if the frame was in use and dirty, write it to disk
         this.pageMap.remove(page.getPageID());
         if (page.isDirty()) {
            this.diskManager.writePage(page.getPageID(), page.getData());
         }
      }

      // read in the page if requested
      page.reset(pageID);
      if (readFromDisk) {
         this.diskManager.readPage(pageID, page.getData());
      }

      // update the page map and notify the replacer
      this.pageMap.put(pageID, page);
      page.incrementPinCount();
      this.replacementPolicy.stateChanged(page.getIndex(), PageState.PINNED);
      return page;
   }

   /**
    * Unpins a disk page from the buffer pool, decreasing its pin count.
    *
    * @param page
    *           page to unpin
    * @param mode
    *           {@link UnpinMode#DIRTY} if the page was modified, {@link UnpinMode#CLEAN} otherwise
    * @throws IllegalArgumentException
    *            if the page is not present or not pinned
    */
   public void unpinPage(final Page< ? > page, final UnpinMode mode) {
      // check the page state
      if (page.getPinCount() == 0) {
         throw new IllegalArgumentException("Page not pinned");
      }
      // update the page
      page.decrementPinCount();
      if (mode == UnpinMode.DIRTY) {
         page.setDirty(true);
      }
      // notify the replacer
      if (page.getPinCount() == 0) {
         this.replacementPolicy.stateChanged(page.getIndex(), PageState.UNPINNED);
      }
   }

   /**
    * Applies the given {@link Function} to the page specified by the given {@link PageID}. The page will be
    * unpinned with the {@link UnpinMode} specified by the function's return value.
    *
    * @param pageID
    *           id of the page to which the function should be applied
    * @param func
    *           function to apply
    * @param <P>
    *           type of the page
    */
   public <P extends PageType> void withPage(final PageID pageID, final Function<Page<P>, UnpinMode> func) {
      final Page<P> page = this.pinPage(pageID);
      UnpinMode mode = UnpinMode.DIRTY;
      try {
         mode = func.apply(page);
      } finally {
         this.unpinPage(page, mode);
      }
   }

   /**
    * Applies a <em>read</em>-operation specified by the given {@link Function} to the page specified by the
    * given {@link PageID}.
    *
    * @param pageID
    *           id of the page to read from
    * @param func
    *           function that specifies the read-operation
    * @param <P>
    *           type of the page to read from
    * @param <R>
    *           type of the function's return value
    * @return result of the read-operation
    */
   public <P extends PageType, R> R readPage(final PageID pageID, final Function<Page<P>, R> func) {
      final Page<P> page = this.pinPage(pageID);
      try {
         return func.apply(page);
      } finally {
         this.unpinPage(page, UnpinMode.CLEAN);
      }
   }

   /**
    * Applies a <em>read/write</em>-operation specified by the given {@link Functio} to the page specified by
    * the given {@link PageID} and returns the result.
    *
    * @param pageID
    *           id of the page
    * @param func
    *           function that specifies the read/write-operation
    * @param <P>
    *           type of the page
    * @param <R>
    *           type of the function's return value
    * @return result of the operation
    */
   public <P extends PageType, R> R readWritePage(final PageID pageID, final Function<Page<P>, R> func) {
      final Page<P> page = this.pinPage(pageID);
      try {
         return func.apply(page);
      } finally {
         this.unpinPage(page, UnpinMode.DIRTY);
      }
   }

   /**
    * Applies a <em>read/write</em>-operation specified by the given {@link Consumer} to the page specified by
    * the given {@link PageID}. If you are interested in the return value of the operation, use the function
    * {@link BufferManager#readWritePage(PageID, Function)} instead.
    *
    * @param pageID
    *           id of the page
    * @param func
    *           function that specifies the read/write-operation
    * @param <P>
    *           type of the page
    */
   public <P extends PageType> void readWritePage(final PageID pageID, final Consumer<Page<P>> func) {
      final Page<P> page = this.pinPage(pageID);
      try {
         func.accept(page);
      } finally {
         this.unpinPage(page, UnpinMode.DIRTY);
      }
   }

   /**
    * Immediately writes a page in the buffer pool to disk, if dirty. Note that flushing a page only writes
    * that page to disk, i.e., the page will not be unpinned, freed, etc.
    *
    * @param page
    *           page to be flushed
    */
   public void flushPage(final Page< ? > page) {
      if (page.isDirty()) {
         // write the page to disk
         this.diskManager.writePage(page.getPageID(), page.getData());
         // the buffer page is now clean
         page.setDirty(false);
      }
   }

   /**
    * Immediately writes all dirty pages in the buffer pool to disk. Note that flushing a page only writes
    * that page to disk, i.e., the page will not be unpinned, freed, etc.
    */
   public void flushAllPages() {
      // iterate the buffer pool
      for (final Page< ? > page : this.pageMap.values()) {
         this.flushPage(page);
      }
   }

   /**
    * Returns the total number of buffer frames.
    *
    * @return total number of buffer frames
    */
   public int getNumBuffers() {
      return this.bufferPool.length;
   }

   /**
    * Returns the total number of pinned buffer frames.
    *
    * @return total number of pinned buffer frames
    */
   public int getNumPinned() {
      int count = 0;
      for (final Page< ? > page : this.pageMap.values()) {
         if (page.getPinCount() > 0) {
            count++;
         }
      }
      return count;
   }

   /**
    * Returns the total number of unpinned buffer frames.
    *
    * @return total number of unpinned buffer frames
    */
   public int getNumUnpinned() {
      return this.getNumBuffers() - this.getNumPinned();
   }
}

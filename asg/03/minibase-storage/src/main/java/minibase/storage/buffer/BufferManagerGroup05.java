/*
 * @(#)BufferManagerGroup05.java   1.0   Nov 06, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

import java.util.Arrays;
import java.util.HashMap;


import minibase.storage.buffer.policy.ReplacementPolicy;
import minibase.storage.file.DiskManager;

/**
 * The BufferManager loads pages from the disk into memory and writes them back to disk when needed.
 *
 * @author Fabian Klopfer, Simon Suckut
 * @version 1.0
 */
public final class BufferManagerGroup05 implements BufferManager {

   /** reference to the DiksManager of the database. */
   private final DiskManager diskManager;
   /** the number of memory pages in the buffer. */
   private final int numBuffers;
   /** reference to the ReplacementPolicy. */
   private final ReplacementPolicy replacementPolicy;
   /** the pages for the BufferManager. */
   private Page< ? >[] bufferPages;
   /** HashMap to link PageIDs to the Buffermanager's pages. */
   private HashMap<PageID, Integer> pageToSlot;
   /** the total number of pinned pages. */
   private int numPagesPinned;

   /**
    * Constructs the BufferManager.
    * 
    * @param diskManager reference to the Database's DiskManager
    * @param bufferPoolSize number of pages the BufferManager should hold
    * @param replacementStrategy reference to a ReplacementPolicy the BufferManager should use
    */
   @SuppressWarnings("rawtypes")
   public BufferManagerGroup05(final DiskManager diskManager, final int bufferPoolSize,
         final ReplacementStrategy replacementStrategy) {
      // initialize local variables
      this.diskManager = diskManager;
      this.numBuffers = bufferPoolSize;
      this.replacementPolicy = replacementStrategy.newInstance(this.numBuffers);
      this.bufferPages = new Page< ? >[bufferPoolSize];
      for (int i = 0; i < bufferPoolSize; i++) {
         this.bufferPages[i] = new Page(i);
      }
      this.pageToSlot = new HashMap<>();
      this.numPagesPinned = 0;
   }

   @Override
   public DiskManager getDiskManager() {
      return this.diskManager;
   }

   @Override
   public Page< ? > newPage() {
      // allocate Page on disk
      final PageID nPageID = this.diskManager.allocatePage();
      // get memory page to load the page into
      final Page<?> nPage = this.pinPage(nPageID);
      // clear all the data in the page
      Arrays.fill(nPage.getData(), (byte) 0);
      // set page to dirty
      nPage.setDirty(true);
      
      return nPage;
   }

   @Override
   public void freePage(final Page< ? > page) {
      // throw exception if page is pined more than once
      if (page.getPinCount() != 1) { 
         throw new IllegalArgumentException(
               "Page was pined " + page.getPinCount() + " times when freePage() was called"); 
      } 
      
      // get PageID and the index of the memory slot
      final PageID pageID = page.getPageID(); 
      final int slotID = this.pageToSlot.get(pageID); 
      
      // remove PageID from HashMap
      this.pageToSlot.remove(pageID); 
      
      // notify replacementPolicy
      this.replacementPolicy.stateChanged(slotID, ReplacementPolicy.PageState.UNPINNED); 
      this.numPagesPinned--; 
      this.replacementPolicy.stateChanged(slotID, ReplacementPolicy.PageState.FREE); 
      
      // remove Page from disk
      this.diskManager.deallocatePage(page.getPageID()); 
      page.setDirty(false);
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T extends PageType> Page<T> pinPage(final PageID pageID) {
      // Check if Page is allready in Memory
      if (this.pageToSlot.containsKey(pageID)) {
         final int pageSlot = this.pageToSlot.get(pageID);
         final Page<T> page = (Page<T>) this.bufferPages[pageSlot];
         
         // notify replacementPolicy if not already done
         if (page.getPinCount() < 1) { 
            this.replacementPolicy.stateChanged(pageSlot, ReplacementPolicy.PageState.PINNED); 
            this.numPagesPinned++; 
         } 
         
         page.incrementPinCount();
         return (Page<T>) page;
         
      // Check if there is any page that can be replaced
      } else if (this.getNumUnpinned() > 0) {
         
         final int victimSlot = this.replacementPolicy.pickVictim();
         final Page<?> victimPage = this.bufferPages[victimSlot];
         
         // cleanly remove victim from memory
         this.pageToSlot.remove(victimPage.getPageID()); 
         flushPage(victimPage);
         victimPage.reset(pageID);
         
         // load new page to memory
         this.diskManager.readPage(pageID, victimPage.getData());
         this.replacementPolicy.stateChanged(victimSlot, ReplacementPolicy.PageState.PINNED);
         this.numPagesPinned++;
         this.pageToSlot.put(pageID, victimSlot);
         victimPage.incrementPinCount();
         
         return (Page<T>) victimPage;
      }
      throw new IllegalStateException("No free pages available");
   }

   @Override
   public void unpinPage(final Page< ? > page, final UnpinMode mode) {
      // throw exception if Page is not loaded
      if (!this.pageToSlot.containsKey(page.getPageID())) {
         throw new IllegalStateException("Page" + page.getPageID() + "not in Memory");
      }

      if (mode == UnpinMode.DIRTY) {
         // the page is now dirty
         page.setDirty(true);
      }

      page.decrementPinCount();
      
      // notify replacementPolicy if needed
      if (page.getPinCount() < 1) {
         final PageID pageID = page.getPageID();
         this.replacementPolicy.stateChanged(this.pageToSlot.get(pageID), ReplacementPolicy.PageState.UNPINNED);
         this.numPagesPinned--;
      }
   }

   @Override
   public void flushPage(final Page< ? > page) {
      if (page.isDirty()) {
         // write the page to disk
         this.diskManager.writePage(page.getPageID(), page.getData());
         // the buffer page is now clean
         page.setDirty(false);
      }
   }

   @Override
   public void flushAllPages() {
      for (Page< ? > page : this.bufferPages) {
         flushPage(page);
      }
   }

   @Override
   public int getNumBuffers() {
      return this.numBuffers;
   }

   @Override
   public int getNumPinned() {
      return this.numPagesPinned;
   }

   @Override
   public int getNumUnpinned() {
      return this.getNumBuffers() - this.getNumPinned();
   }
}

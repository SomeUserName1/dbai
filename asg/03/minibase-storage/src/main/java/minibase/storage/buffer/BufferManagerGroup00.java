/*
 * @(#)BufferManagerGroup00.java   1.0   Nov 06, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

import minibase.storage.buffer.policy.ReplacementPolicy;
import minibase.storage.file.DiskManager;

import java.util.Arrays;
import java.util.HashMap;

public final class BufferManagerGroup00 implements BufferManager {

   private final DiskManager diskManager;
   private final int numBuffers;
   private final ReplacementPolicy replacementPolicy;
   private Page< ? >[] bufferPages;
   private HashMap<PageID, Integer> pageToSlot;

   public BufferManagerGroup00(final DiskManager diskManager, final int bufferPoolSize,
         final ReplacementStrategy replacementStrategy) {
      this.diskManager = diskManager;
      this.numBuffers = bufferPoolSize;
      this.replacementPolicy = replacementStrategy.newInstance(this.numBuffers);
      this.bufferPages = new Page< ? >[bufferPoolSize];
      this.pageToSlot = new HashMap<>();
   }

   @Override
   public DiskManager getDiskManager() {
      return this.diskManager;
   }

   @Override
   public Page< ? > newPage() {
      final PageID nPageID = this.diskManager.allocatePage();
      final Page<?> nPage = this.pinPage(nPageID);
      Arrays.fill(nPage.getData(), (byte) 0);
      nPage.setDirty(true);
      //flushPage(nPage); already done by pinPage()
      return nPage;
   }

   @Override
   public void freePage(final Page< ? > page) {
      if (page.getPinCount() != 1) { //
         throw new IllegalArgumentException("Page must be pined exactly once when freePage() ist called"); //
      } //
      final PageID pageID = page.getPageID(); //
      final int slotID = pageToSlot.get(pageID); //
      pageToSlot.remove(pageID); //
      replacementPolicy.stateChanged(slotID, ReplacementPolicy.PageState.FREE); //
      diskManager.deallocatePage(page.getPageID()); //
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T extends PageType> Page<T> pinPage(final PageID pageID) {
      if (pageToSlot.containsKey(pageID)) {
         final int pageSlot = pageToSlot.get(pageID);
         final Page<T> page = (Page<T>) bufferPages[pageSlot];
         if(page.getPinCount() < 1) { //
            replacementPolicy.stateChanged(pageSlot, ReplacementPolicy.PageState.PINNED); //
         } //
         page.incrementPinCount();
         return page;
      } else if (this.getNumUnpinned() > 1) {
         final int victimSlot = replacementPolicy.pickVictim();
         final Page<?> victimPage = bufferPages[victimSlot];
         pageToSlot.remove(victimPage.getPageID()); //
         flushPage(victimPage);
         victimPage.reset(pageID);
         diskManager.readPage(pageID, victimPage.getData());
         replacementPolicy.stateChanged(victimSlot, ReplacementPolicy.PageState.PINNED);
         pageToSlot.put(pageID, victimSlot);
         victimPage.incrementPinCount();
         return (Page<T>) victimPage;
      }
      throw new IllegalStateException("No free pages available");
   }

   @Override
   public void unpinPage(final Page< ? > page, final UnpinMode mode) {
      if (!pageToSlot.containsKey(page.getPageID())) {
         throw new IllegalStateException("Page" + page.getPageID() + "not in Memory");
      }

      if (mode == UnpinMode.CLEAN) {
         //page.setDirty(false); what if the page was dirty before?
      } else {
         page.setDirty(true);
      }

      page.decrementPinCount();
      if (page.getPinCount() < 1) {
         final PageID pageID = page.getPageID();
         replacementPolicy.stateChanged(pageToSlot.get(pageID), ReplacementPolicy.PageState.UNPINNED);
         //pageToSlot.remove(pageID); 
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
      return pageToSlot.size();
   }

   @Override
   public int getNumUnpinned() {
      return this.getNumBuffers() - this.getNumPinned();
   }
}

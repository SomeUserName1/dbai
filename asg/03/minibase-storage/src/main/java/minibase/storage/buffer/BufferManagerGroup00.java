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

import minibase.storage.file.DiskManager;
import java.util.HashMap;

public final class BufferManagerGroup00 implements BufferManager {

   private final DiskManager diskManager;
   private final int numBuffers;
   private final ReplacementStrategy replacementStrategy;
   private Page<?>[] bufferPages;
   private HashMap<PageID, Integer> pageToFrameMapping;



   public BufferManagerGroup00(final DiskManager diskManager, final int bufferPoolSize,
         final ReplacementStrategy replacementStrategy) {
      this.diskManager = diskManager;
      this.numBuffers = bufferPoolSize;
      this.replacementStrategy = replacementStrategy;
      this.bufferPages = new Page<?>[bufferPoolSize];
      this.pageToFrameMapping = new HashMap<>();

   }

   @Override
   public DiskManager getDiskManager() { return this.diskManager; }

   @Override
   public Page<?> newPage() {
      PageID nPageID = this.diskManager.allocatePage();
      Page nPage = this.pinPage(nPageID);
      System.arraycopy(new Byte[DiskManager.PAGE_SIZE], 0, nPage.getData(), 0, DiskManager.PAGE_SIZE);
      nPage.setDirty(true);
      flushPage(nPage);
      return nPage;
   }

   @Override
   public void freePage(final Page<?> page) {
      flushPage(page);
      this.diskManager.deallocatePage(page.getPageID());
   }

   @Override
   public <T extends PageType> Page<T> pinPage(final PageID pageID) {
       if(!pageID.isValid()) throw new IllegalStateException();
      // TODO implement method
      throw new UnsupportedOperationException("Not yet implemented.");
   }

   @Override
   public void unpinPage(final Page<?> page, final UnpinMode mode) {
      // TODO implement method
      throw new UnsupportedOperationException("Not yet implemented.");
   }

   @Override
   public void flushPage(final Page<?> page) {
      if (page.isDirty()) {
         // write the page to disk
         this.getDiskManager().writePage(page.getPageID(), page.getData());
         // the buffer page is now clean
         page.setDirty(false);
      }
   }

   @Override
   public void flushAllPages() {
      for( Page page : this.bufferPages) flushPage(page);
   }

   @Override
   public int getNumBuffers() { return this.numBuffers; }

   @Override
   public int getNumPinned() {
       int sum = 0;
       for ( int bufPageNr : this.pageToFrameMapping.values())
           sum += bufPageNr < 0 ? 1 : 0;
       return sum;
   }

   @Override
   public int getNumUnpinned() {
      return this.getNumBuffers() - this.getNumPinned();
   }


}

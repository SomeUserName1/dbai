/*
 * @(#)HashDirectoryIterator.java   1.0   Nov 16, 2015
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
import java.util.NoSuchElementException;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * Iterator over directory entries, which are represented as the page ids of the first page of the corresponding 
 * hash bucket.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni.kn&gt;
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
public class HashDirectoryIterator implements Iterator<PageID> {
   
   /** Buffer manager. */
   private BufferManager bufferManager;
   /** Current directory page id. */
   private PageID dirId;
   /** Current directory slot. */
   private int currSlot;
   /** Next page id. */
   private PageID currId;

   /**
    * Constructor.
    * @param bufferManager
    *              buffer manager
    * @param headerId
    *              first directory page id
    */
   public HashDirectoryIterator(final BufferManager bufferManager, final PageID headerId) {
      this.bufferManager = bufferManager;
      this.dirId = headerId;
      this.currSlot  = 0;
      this.moveToNext();
   }
   
   /**
    * Advances the pointer to the next page Id that can be returned or {@link PageID.INVALID} if no such entry exists.
    */
   private void moveToNext() {
      do { 
         Page<HashDirectoryPage> page = this.bufferManager.pinPage(this.dirId);
         if (this.currSlot == HashDirectoryPage.getWritableSize(page) / PageID.SIZE) {
            this.dirId = HashDirectoryPage.getNextPagePointer(page);
            this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
            if (!this.dirId.isValid()) {
               this.currId = PageID.INVALID;
               return;
            }
            page = this.bufferManager.pinPage(this.dirId);
            this.currSlot = 0;
         }
         this.currId = HashDirectoryPage.readPageID(page, this.currSlot);
         this.currSlot++;
      } while (!this.currId.isValid());
   }

   @Override
   public boolean hasNext() {
      return this.currId.isValid();
   }

   @Override
   public PageID next() {
      if (!this.hasNext()) {
         throw new NoSuchElementException();
      }
      final PageID result = this.currId;
      this.moveToNext();
      return result;
   }
}


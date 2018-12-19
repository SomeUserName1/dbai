/*
 * @(#)HeapFile.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import java.util.Objects;
import java.util.Optional;

import minibase.RecordID;
import minibase.query.evaluator.TupleIterator;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * A heap file is an unordered set of records, stored on a set of pages. This class provides basic support for
 * inserting, selecting, updating, and deleting records.
 *
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 */
public final class HeapFile implements File {

   /** Page type for directory pages. */
   static final short DIR_PAGE = 10;

   /** Page type for data pages. */
   static final short DATA_PAGE = 11;

   /** The heap file entry name. */
   private final String name;

   /** Header page of the directory. */
   private PageID headID;

   /**
    * Pointer to the first directory page with free space and pointer to the slot inside this directory page,
    * which points to the data page with space.
    */
   private RecordID lastInsert;

   /** Reference to the buffer manager. */
   private final BufferManager bufferManager;

   /**
    * Hidden constructor.
    *
    * @param bufferManager
    *           this file's buffer manager
    * @param name
    *           this file's name, {@code null} if it is a temporary file
    * @param headID
    *           ID of the header page
    */
   private HeapFile(final BufferManager bufferManager, final String name, final PageID headID) {
      this.bufferManager = Objects.requireNonNull(bufferManager);
      this.name = name;
      this.headID = Objects.requireNonNull(headID);
      this.lastInsert = new RecordID(this.headID, 0);
   }

   /**
    * Opens an existing heap file.
    *
    * @param bufferManager
    *           the buffer manager
    * @param name
    *           the file's name, must be non-{@code null}
    * @return the heap file
    * @throws NullPointerException
    *            if {@code name} is {@code null}
    * @throws IllegalArgumentException
    *            if the file does not exist
    */
   public static HeapFile open(final BufferManager bufferManager, final String name) {
      final PageID headID = bufferManager.getDiskManager().getFileEntry(Objects.requireNonNull(name));
      if (headID == null) {
         throw new IllegalArgumentException("Heap file " + name + " does not exist.");
      }

      return new HeapFile(bufferManager, name, headID);
   }

   /**
    * Creates a new heap file.
    *
    * @param bufferManager
    *           the buffer manager
    * @param name
    *           the file's name, {@code null} for a temporary file
    * @return the newly created heap file
    * @throws IllegalArgumentException
    *            if a file with the given name already exists
    */
   public static HeapFile create(final BufferManager bufferManager, final String name) {
      if (name != null) {
         final PageID oldID = bufferManager.getDiskManager().getFileEntry(name);
         if (oldID != null) {
            throw new IllegalArgumentException("Heap file " + name + " already exists.");
         }
      }

      // allocate and init the page
      final Page<HeapFileDirectoryPage> dirPage = HeapFileDirectoryPage.newPage(bufferManager);
      final PageID headID = dirPage.getPageID();
      bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);

      if (name != null) {
         // create the file entry
         bufferManager.getDiskManager().addFileEntry(name, headID);
      }

      return new HeapFile(bufferManager, name, headID);
   }

   /**
    * Creates a new temporary heap file.
    *
    * @param bufferManager
    *           the buffer manager
    * @return the new temporary heap file
    */
   public static HeapFile createTemporary(final BufferManager bufferManager) {
      return create(bufferManager, null);
   }

   /**
    * The heap file's buffer manager.
    *
    * @return the buffer manager
    */
   protected BufferManager getBufferManager() {
      return this.bufferManager;
   }

   /**
    * Gets the heap file's first page.
    *
    * @return the file's first page
    */
   public PageID getHeadID() {
      return this.headID;
   }

   /**
    * Returns the name of this heap file.
    *
    * @return file name
    */
   public Optional<String> getName() {
      return Optional.of(this.name);
   }

   @Override
   public void delete() {
      if (!this.headID.isValid()) {
         throw new IllegalStateException("File already deleted.");
      }
      // for each directory page
      PageID dirID = this.headID;
      while (dirID.isValid()) {
         // pin the page, get the entry count
         final Page<HeapFileDirectoryPage> dirPage = this.bufferManager.pinPage(dirID);
         final int count = HeapFileDirectoryPage.getEntryCount(dirPage);
         // free all heap pages
         for (int i = 0; i < count; i++) {
            final PageID pageID = HeapFileDirectoryPage.getPageID(dirPage, i);
            this.bufferManager.freePage(this.bufferManager.pinPage(pageID));
         }
         // get the next directory page and free the current directory page
         dirID = HeapFilePage.getNextPage(dirPage);
         this.bufferManager.freePage(dirPage);
      }
      // remove the file entry, if applicable
      if (this.name != null) {
         this.bufferManager.getDiskManager().deleteFileEntry(this.name);
      }
      this.headID = PageID.INVALID;
   }

   @Override
   public int bulkLoad(final TupleIterator iterator, final int recordLength) {
      int totalCount = 0;

      // searches for free space
      PageID pageID = null;
      while (iterator.hasNext()) {

         pageID = this.getAvailablePage(recordLength);
         // pin data page and calculate how many records fit on it
         final Page<HeapFilePage> page = this.bufferManager.pinPage(pageID);
         final int recSpace = HeapFilePage.getFreeSpace(page) / (recordLength + HeapFilePage.SLOT_SIZE);

         // reads as many lines as fit on the page or until eof
         int recCount = 0;
         while (iterator.hasNext() && recCount < recSpace) {
            final byte[] tuple = iterator.next();
            if (tuple != null) {
               // insert tuple
               HeapFilePage.insertRecord(page, tuple);
               totalCount++;
               recCount++;
            }
         }

         // write out the data pages if something was added
         if (recCount > 0) {
            // get the new fill status
            final short freecnt = HeapFilePage.getFreeSpace(page);

            // pin the directory page
            final Page<HeapFileDirectoryPage> dirPage = this.bufferManager
                  .pinPage(this.lastInsert.getPageID());
            // add data page to directory
            if (!HeapFileDirectoryPage.getPageID(dirPage, this.lastInsert).isValid()) {
               HeapFilePage.setDirectoryRecordID(page, this.lastInsert);
               HeapFileDirectoryPage.setPageID(dirPage, this.lastInsert, page.getPageID());
               HeapFileDirectoryPage.setEntryCount(dirPage,
                     (short) (1 + HeapFileDirectoryPage.getEntryCount(dirPage)));
            }
            this.bufferManager.unpinPage(page, UnpinMode.DIRTY);

            // update directory page with new status
            HeapFileDirectoryPage.setRecordCount(dirPage, this.lastInsert,
                  (short) (recCount + HeapFileDirectoryPage.getRecordCount(dirPage, this.lastInsert)));
            HeapFileDirectoryPage.setFreeCount(dirPage, this.lastInsert, (short) freecnt);

            this.bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
         } else {
            final short freecnt = HeapFilePage.getFreeSpace(page);
            this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

            // if data page is empty delete it
            if (HeapFilePage.DATA_CONTENT_SIZE == freecnt) {
               // pin the directory page
               final Page<HeapFileDirectoryPage> dirPage = this.bufferManager
                     .pinPage(this.lastInsert.getPageID());
               // delete the empty data page
               this.deletePage(pageID, dirPage, this.lastInsert);
            }
         }
      }

      return totalCount;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public RecordID insertRecord(final byte[] record) {
      // make sure the record isn't too long
      if (record.length > HeapFilePage.DATA_CONTENT_SIZE) {
         throw new IllegalArgumentException("Record too large");
      }

      // get an available page and pin it
      final PageID pageID = this.getAvailablePage(record.length /* + HeapFilePage.SLOT_SIZE */);
      final Page<HeapFilePage> page = this.bufferManager.pinPage(pageID);

      // insert the record and unpin the dirty page
      final RecordID rid = HeapFilePage.insertRecord(page, record);
      final short freecnt = HeapFilePage.getFreeSpace(page);
      this.bufferManager.unpinPage(page, UnpinMode.DIRTY);

      // update the directory entry and return the new RID
      this.updateEntry(pageID, 1, freecnt);
      return rid;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public byte[] selectRecord(final RecordID rid) {

      // pin the data page of the record
      final Page<HeapFilePage> page = this.bufferManager.pinPage(rid.getPageID());
      try {
         // select the record
         return HeapFilePage.selectRecord(page, rid);
      } finally {
         // unpin the clean page
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void updateRecord(final RecordID rid, final byte[] newRecord) {

      // pin the data page of the record
      final Page<HeapFilePage> page = this.bufferManager.pinPage(rid.getPageID());
      try {
         // update the record and unpin the dirty page
         HeapFilePage.updateRecord(page, rid, newRecord);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
      } catch (final IllegalArgumentException exc) {
         // unpin the clean page and forward the exception
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
         throw exc;
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void deleteRecord(final RecordID rid) {
      // pin the data page of the record
      final Page<HeapFilePage> page = this.bufferManager.pinPage(rid.getPageID());

      // save the old record length
      try {
         // delete the record and unpin the dirty page
         HeapFilePage.deleteRecord(page, rid);
         final short freecnt = HeapFilePage.getFreeSpace(page);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);

         // update the directory entry
         this.updateEntry(rid.getPageID(), -1, freecnt);
      } catch (final IllegalArgumentException exc) {

         // unpin the clean page and forward the exception
         this.bufferManager.unpinPage(page, UnpinMode.CLEAN);
         throw exc;
      }
   }

   /**
    * Gets the number of records in the file.
    *
    * @return number of records
    */
   protected int getRecordCount() {

      // initialize the default count
      int cnt = 0;

      // for each directory page
      PageID dirID = this.headID;
      while (dirID.isValid()) {
         // pin the page, get the entry count
         final Page<HeapFileDirectoryPage> dirPage = this.bufferManager.pinPage(dirID);
         final int count = HeapFileDirectoryPage.getEntryCount(dirPage);

         // sum all record counts
         for (int i = 0; i < count; i++) {
            cnt += HeapFileDirectoryPage.getRecordCount(dirPage, new RecordID(dirID, i));
         }

         // get the next dir page and unpin the current
         dirID = HeapFilePage.getNextPage(dirPage);
         this.bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      }

      // return the count
      return cnt;
   }

   @Override
   public FileScan openScan() {
      return new HeapFileScan();
   }

   /**
    * Returns the name of the heap file.
    *
    * @return the heap file's name
    */
   @Override
   public String toString() {
      return this.name;
   }

   /**
    * Searches the directory for a data page with enough free space to store a record of the given size. If no
    * suitable page is found, this creates a new data page.
    *
    * @param reclen
    *           length of the record to insert
    * @return page ID of the page to insert into
    */
   PageID getAvailablePage(final int reclen) {
      this.lastInsert = this.findDirPageWithSpace(reclen, this.lastInsert);

      // search the directory for a data page with enough space
      final PageID dirID = this.lastInsert.getPageID();
      // pin the page, get the entry pageID
      final Page<HeapFileDirectoryPage> dirPage = this.bufferManager.pinPage(dirID);
      final PageID freeID = HeapFileDirectoryPage.getPageID(dirPage, this.lastInsert);
      this.bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
      // in case data page was not initialized yet
      if (!freeID.isValid()) {
         // create a new data page
         return this.insertPage();
      }
      return freeID;
   }

   /**
    * Finds a directory page, which contains a reference to a page with enough space for the record.
    * 
    * @param reclen
    *           the record length
    * @param startID
    *           the page to start the search
    * @return [dirPageID, entryIndex]
    */
   private RecordID findDirPageWithSpace(final int reclen, final RecordID startID) {
      // search the directory for a data page with enough space
      PageID dirID = startID.getPageID();
      int slotStart = startID.getSlotNo();
      while (dirID.isValid()) {
         // pin the page, get the entry count
         final Page<HeapFileDirectoryPage> dirPage = this.bufferManager.pinPage(dirID);

         // check each page's free space
         for (int i = slotStart; i < HeapFileDirectoryPage.MAX_ENTRIES; i++) {
            final RecordID rid = new RecordID(dirID, i);
            // search not full or uninitialized data page
            if (!HeapFileDirectoryPage.getPageID(dirPage, rid).isValid()
                  || HeapFileDirectoryPage.getFreeCount(dirPage, rid) >= reclen + HeapFilePage.SLOT_SIZE) {
               this.bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
               return rid;
            }
         }

         // get the next dir page and unpin the current
         final PageID nextID = HeapFilePage.getNextPage(dirPage);
         // no suitable data page was found, create a new one
         if (!nextID.isValid()) {
            // create the new directory page
            final Page<HeapFileDirectoryPage> newDirPage = HeapFileDirectoryPage.newPage(this.bufferManager);
            final PageID newDirID = newDirPage.getPageID();

            // link it into the page list
            HeapFilePage.setNextPage(dirPage, newDirID);
            HeapFilePage.setPrevPage(newDirPage, dirID);
            this.bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
            this.bufferManager.unpinPage(newDirPage, UnpinMode.DIRTY);

            return new RecordID(newDirID, 0);
         }
         this.bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
         dirID = nextID;
         slotStart = 0;
      }
      // never reached
      throw new IllegalStateException();
   }

   /**
    * Updates the directory entry for the given page with the recently changed values (i.e. delta is the
    * difference between the old and the new).
    *
    * @param pageID
    *           ID of the page to update
    * @param deltaRec
    *           difference between old and new number of records
    * @param freecnt
    *           new free count
    */
   void updateEntry(final PageID pageID, final int deltaRec, final int freecnt) {
      // finds the directory page of the updated data page
      final Page<HeapFilePage> page = this.bufferManager.pinPage(pageID);
      final RecordID rid = HeapFilePage.getDirectoryRecordID(page);
      this.bufferManager.unpinPage(page, UnpinMode.CLEAN);

      // pins directory page
      final Page<HeapFileDirectoryPage> dirPage = this.bufferManager.pinPage(rid.getPageID());

      // if the data page is now empty, delete it
      final int reccnt = HeapFileDirectoryPage.getRecordCount(dirPage, rid) + deltaRec;
      if (reccnt < 1) {
         // call the helper method
         this.deletePage(pageID, dirPage, rid);
      } else {
         // otherwise, update it and unpin the dirty directory page
         HeapFileDirectoryPage.setRecordCount(dirPage, rid, (short) reccnt);
         HeapFileDirectoryPage.setFreeCount(dirPage, rid, (short) freecnt);
         this.bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
      }
   }

   /**
    * Inserts a new data page its directory entry into the heap file. If necessary, this also inserts a new
    * directory page.
    *
    * @return id of the new data page
    */
   PageID insertPage() {

      // search the directory for a non-full page
      int count;
      // start search at last insert
      PageID dirID = this.lastInsert.getPageID();
      int index = this.lastInsert.getSlotNo();
      Page<HeapFileDirectoryPage> dirPage;
      do {
         // pin the page, see if it's full
         dirPage = this.bufferManager.pinPage(dirID);
         count = HeapFileDirectoryPage.getEntryCount(dirPage);
         if (count < HeapFileDirectoryPage.MAX_ENTRIES) {
            // search for a free data page
            for (int i = index; i < count; i++) {
               this.lastInsert = new RecordID(dirID, i);
               if (!HeapFileDirectoryPage.getPageID(dirPage, this.lastInsert).isValid()) {
                  break;
               }
            }
            break;
         }

         // if no suitable directory page was found, create a new one
         final PageID nextID = HeapFilePage.getNextPage(dirPage);
         if (!nextID.isValid()) {
            // create the new directory page
            final Page<HeapFileDirectoryPage> newDirPage = HeapFileDirectoryPage.newPage(this.bufferManager);
            final PageID newDirID = newDirPage.getPageID();

            // link it into the page list
            HeapFilePage.setNextPage(dirPage, newDirID);
            HeapFilePage.setPrevPage(newDirPage, dirID);
            this.bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);

            // use the new directory page
            dirID = newDirID;
            dirPage = newDirPage;
            this.lastInsert = new RecordID(dirID, 0);
            break;
         }

         // unpin the current and go to the next
         this.bufferManager.unpinPage(dirPage, UnpinMode.CLEAN);
         dirID = nextID;
         index = 0;
      } while (true);

      // create the new data page
      final Page<HeapFilePage> dataPage = HeapFilePage.newPage(this.bufferManager, HeapFile.DATA_PAGE);
      final PageID dataID = dataPage.getPageID();

      // link to directory page
      HeapFilePage.setDirectoryRecordID(dataPage, this.lastInsert);

      // create the new directory entry
      HeapFileDirectoryPage.setPageID(dirPage, this.lastInsert, dataID);
      HeapFileDirectoryPage.setRecordCount(dirPage, this.lastInsert, (short) 0);
      HeapFileDirectoryPage.setFreeCount(dirPage, this.lastInsert, HeapFilePage.getFreeSpace(dataPage));
      HeapFileDirectoryPage.setEntryCount(dirPage, (short) ++count);

      // unpin the pages and return the new data page's id
      this.bufferManager.unpinPage(dataPage, UnpinMode.DIRTY);
      this.bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
      return dataID;
   }

   /**
    * Deletes the given data page and its directory entry from the heap file. If appropriate, this also deletes
    * the directory page.
    *
    * @param pageID
    *           page to delete
    * @param dirPage
    *           directory page
    * @param rid
    *           record id of the data page inside the directory
    */
   private void deletePage(final PageID pageID, final Page<HeapFileDirectoryPage> dirPage, final RecordID rid) {

      // delete the data page and directory entry
      this.bufferManager.freePage(this.bufferManager.pinPage(pageID));
      // set reference to null
      HeapFileDirectoryPage.deleteReference(dirPage, rid);

      // delete empty, non-head directory pages
      short count = HeapFileDirectoryPage.getEntryCount(dirPage);
      if (count == 1 && !dirPage.getPageID().equals(this.headID)) {
         // remove the page from the list
         final PageID prevID = HeapFilePage.getPrevPage(dirPage);
         final PageID nextID = HeapFilePage.getNextPage(dirPage);
         if (prevID.isValid()) {
            final Page<HeapFilePage> prev = this.bufferManager.pinPage(prevID);
            HeapFilePage.setNextPage(prev, nextID);
            this.bufferManager.unpinPage(prev, UnpinMode.DIRTY);
         }

         if (nextID.isValid()) {
            final Page<HeapFilePage> next = this.bufferManager.pinPage(nextID);
            HeapFilePage.setPrevPage(next, prevID);
            this.bufferManager.unpinPage(next, UnpinMode.DIRTY);
         }

         // free the empty directory page
         this.bufferManager.freePage(dirPage);
      } else {
         // otherwise, update the count and unpin the page
         HeapFileDirectoryPage.setEntryCount(dirPage, --count);
         this.bufferManager.unpinPage(dirPage, UnpinMode.DIRTY);
      }

      // invalidate last insert pointer
      this.lastInsert = new RecordID(this.headID, 0);
   }

   @Override
   public void close() {
      // temporary files get deleted when they are closed
      // this way, when a try-with-resource block ends, the temporary file is gone :)
      if (this.name == null) {
         this.delete();
      }
   }

   /**
    * Scan over the heap file contexts.
    */
   private final class HeapFileScan implements FileScan {

      /** Currently pinned directory page (outer loop). */
      private Page<HeapFileDirectoryPage> dirPage;

      /** Number of entries on the directory page. */
      private int count;

      /** Index of the current entry on the directory page. */
      private int index;

      /** Currently pinned data page (inner loop). */
      private Page<HeapFilePage> dataPage;

      /** RID of the last returned record on the data page. */
      private RecordID lastRID;

      /**
       * Constructs a file scan by pinning the directory header page and initializing iterator fields.
       */
      private HeapFileScan() {
         this.reset();
      }

      @Override
      public boolean hasNext() {
         // if iterating on a data page
         if (this.lastRID != null) {
            if (HeapFilePage.nextRecord(this.dataPage, this.lastRID) != null) {
               return true;
            }
         }
         // if more data page
         if (this.index < this.count - 1) {
            return true;
         }
         // if more directory pages
         if (HeapFilePage.getNextPage(this.dirPage).isValid()) {
            return true;
         }
         // none of the above
         return false;
      }

      @Override
      public byte[] next() {
         final BufferManager bufferManager = HeapFile.this.getBufferManager();
         // base case: iterate within the data page
         if (this.lastRID != null) {
            // get the next record id
            final RecordID nextID = HeapFilePage.nextRecord(this.dataPage, this.lastRID);
            if (nextID != null) {
               // return both the RID and the record
               this.lastRID = nextID;
               return HeapFilePage.selectRecord(this.dataPage, nextID);
            }
            // all done with the current data page
            bufferManager.unpinPage(this.dataPage, UnpinMode.CLEAN);
            this.dataPage = null;
            this.lastRID = null;
         }

         for (;;) {
            // move on to the next data page
            if (++this.index < this.count) {
               // pin the next data page
               final PageID dataID = HeapFileDirectoryPage.getPageID(this.dirPage, this.index);
               this.dataPage = bufferManager.pinPage(dataID);
               // reset the counter and get the first record
               final RecordID nextID = HeapFilePage.firstRecord(this.dataPage);
               if (nextID != null) {
                  this.lastRID = nextID;
                  return HeapFilePage.selectRecord(this.dataPage, nextID);
               }
            }

            // move on to the next directory page
            final PageID nextDirID = HeapFilePage.getNextPage(this.dirPage);
            if (!nextDirID.isValid()) {
               // otherwise, no more records
               throw new IllegalStateException("No more elements");
            }

            // unpin the current dir page, pin the next dir page
            bufferManager.unpinPage(this.dirPage, UnpinMode.CLEAN);
            this.dirPage = bufferManager.pinPage(nextDirID);

            // reset the counters and try again
            this.count = HeapFileDirectoryPage.getEntryCount(this.dirPage);
            this.index = -1;
         }
      }

      @Override
      public RecordID lastID() {
         if (this.lastRID == null) {
            throw new IllegalStateException("No record was returned since the last reset.");
         }
         return this.lastRID;
      }

      @Override
      public void updateLast(final byte[] newRecord) {
         final RecordID last = this.lastID();
         final Page<HeapFilePage> dataPage = HeapFile.this.bufferManager.pinPage(last.getPageID());
         HeapFilePage.updateRecord(dataPage, last, newRecord);
         HeapFile.this.bufferManager.unpinPage(dataPage, UnpinMode.DIRTY);
      }

      @Override
      public void reset() {
         final BufferManager bufferManager = HeapFile.this.getBufferManager();
         if (this.dataPage != null) {
            bufferManager.unpinPage(this.dataPage, UnpinMode.CLEAN);
            this.dataPage = null;
         }
         if (this.dirPage != null) {
            bufferManager.unpinPage(this.dirPage, UnpinMode.CLEAN);
         }

         // pin the head page and get the count
         this.dirPage = bufferManager.pinPage(HeapFile.this.getHeadID());
         this.count = HeapFileDirectoryPage.getEntryCount(this.dirPage);

         // initialize other data fields
         this.index = -1;
         this.lastRID = null;
      }

      @Override
      public void close() {
         // unpin the pages where applicable
         final BufferManager bufferManager = HeapFile.this.getBufferManager();
         if (this.dataPage != null) {
            bufferManager.unpinPage(this.dataPage, UnpinMode.CLEAN);
            this.dataPage = null;
         }
         if (this.dirPage != null) {
            bufferManager.unpinPage(this.dirPage, UnpinMode.CLEAN);
            this.dirPage = null;
         }
         // invalidate the other fields
         this.count = -1;
         this.index = -1;
         this.lastRID = null;
      }
   }
}

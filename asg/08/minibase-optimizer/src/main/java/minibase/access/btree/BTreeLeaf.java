/*
 * @(#)BTreeLeaf.java   1.0   Oct 29, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.btree;

import java.util.Arrays;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;

/**
 * Utility class for accessing fields in a {@link BTreeIndex} leaf page.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
final class BTreeLeaf extends BTreePage {

   /** Hidden default constructor. */
   private BTreeLeaf() {
      throw new AssertionError();
   }

   /**
    * Initializes a newly allocated page as a B+-Tree leaf page.
    *
    * @param bufferManager the buffer manager for getting the new page
    * @param prev
    *           previous page's ID
    * @param next
    *           next page's ID
    * @return the leaf page
    */
   static Page<BTreeLeaf> newPage(final BufferManager bufferManager, final PageID prev, final PageID next) {
      @SuppressWarnings("unchecked")
      final Page<BTreeLeaf> leaf = (Page<BTreeLeaf>) bufferManager.newPage();
      BTreePage.setMeta(leaf, true, 0);
      BTreeLeaf.setPrevPage(leaf, prev);
      BTreeLeaf.setNextPage(leaf, next);
      return leaf;
   }

   /**
    * Checks if the given page is a leaf page.
    * @param page the page to check
    * @return the page if it is a leaf, {@code null} otherwise
    */
   static Page<BTreeLeaf> check(final Page<BTreePage> page) {
      return cast(BTreePage.isLeafPage(page) ? page : null);
   }

   /**
    * Casts the given page to a leaf page.
    * @param page the page to cast
    * @return the page
    */
   @SuppressWarnings("unchecked")
   static Page<BTreeLeaf> cast(final Page<? extends BTreePage> page) {
      return (Page<BTreeLeaf>) page;
   }

   /**
    * Gets the key stored at the given position.
    * @param page the page
    * @param pos position of the key
    * @param keyType type of the key as byte
    * @return the key's value
    */
   static SearchKey getKey(final Page<BTreeLeaf> page, final int pos, final SearchKeyType keyType) {
      return SearchKey.getKey(page, keyType, keyOffset(pos, keyType.getKeyLength()));
   }

   /**
    * Sets the key at the given position.
    * @param page the page
    * @param pos position of the key
    * @param key the key to set
    * @param keySize size of the key in bytes
    */
   static void setKey(final Page<BTreeLeaf> page, final int pos, final SearchKey key, final int keySize) {
      final int offset = keyOffset(pos, keySize);
      // make sure the old data is gone
      Arrays.fill(page.getData(), offset, offset + keySize, (byte) 0);
      key.writeRawData(page.getData(), offset);
   }

   /**
    * Gets the record ID at the given position in this page.
    * @param page the page
    * @param pos position of the record ID
    * @param keySize size of the key
    * @return the record ID
    */
   static RecordID getRecordID(final Page<BTreeLeaf> page, final int pos, final int keySize) {
      return page.readRecordID(recordIDOffset(pos, keySize));
   }

   /**
    * Sets the record ID at the given position in this page.
    * @param page the page
    * @param pos position of the record ID
    * @param value record ID to write
    * @param keySize size of the key
    */
   static void setRecordID(final Page<BTreeLeaf> page, final int pos, final RecordID value, final int keySize) {
      page.writeRecordID(recordIDOffset(pos, keySize), value);
   }

   /**
    * Inserts an entry (i.e. a pair of a key and a record ID) at the given position into this leaf page.
    * @param page the page
    * @param pos insertion position
    * @param key key to insert
    * @param value record ID to insert
    * @param keySize size of the key as byte
    */
   static void insertEntry(final Page<BTreeLeaf> page, final int pos, final SearchKey key, final RecordID value,
         final int keySize) {
      final int numKeys = BTreePage.getNumKeys(page);
      final int offset = keyOffset(pos, keySize);
      final byte[] data = page.getData();
      // make space for new entry
      System.arraycopy(data, offset, data, offset + keySize
      + RecordID.BYTES, (numKeys - pos) * (keySize + RecordID.BYTES));
      // write entry
      // write the key value
      setKey(page, pos, key, keySize);
      page.writeRecordID(offset + keySize, value);
      BTreePage.setNumKeys(page, numKeys + 1);
   }

   /**
    * Deletes the entry (i.e. a pair of a key and a record ID) from the given position in this page.
    * @param page the page
    * @param pos position of the entry to delete
    * @param keySize size of the key
    */
   static void deleteEntry(final Page<BTreeLeaf> page, final int pos, final int keySize) {
      final int numKeys = BTreePage.getNumKeys(page);
      final int offset = keyOffset(pos, keySize);
      final byte[] data = page.getData();
      System.arraycopy(data, offset + (keySize + RecordID.BYTES), data, offset,
            (numKeys - pos - 1) * (keySize + RecordID.BYTES));
      BTreePage.setNumKeys(page, numKeys - 1);
   }

   /**
    * Gets the previous-page ID stored in this page.
    * @param page the page
    * @return the next-page ID
    */
   static PageID getPrevPage(final Page<BTreeLeaf> page) {
      return page.readPageID(BTreePage.META_POS - 2 * PageID.BYTES);
   }

   /**
    * Sets the previous-page ID of this page.
    * @param page the page
    * @param id new ID
    */
   static void setPrevPage(final Page<BTreeLeaf> page, final PageID id) {
      page.writePageID(BTreePage.META_POS - 2 * PageID.BYTES, id);
   }

   /**
    * Gets the next-page ID stored in this page.
    * @param page the page
    * @return the previous-page ID
    */
   static PageID getNextPage(final Page<BTreeLeaf> page) {
      return page.readPageID(BTreePage.META_POS - PageID.BYTES);
   }

   /**
    * Sets the next-page ID of this page.
    * @param page the page
    * @param id new ID
    */
   static void setNextPage(final Page<BTreeLeaf> page, final PageID id) {
      page.writePageID(BTreePage.META_POS - PageID.BYTES, id);
   }

   /**
    * Shifts {@code n} entries from position {@code from} to position {@code to} in the given page.
    * @param page the page
    * @param from original start of the entries
    * @param to new start of the entries
    * @param n number of entries to shift
    * @param keySize size of the key
    */
   static void shiftEntries(final Page<BTreeLeaf> page, final int from, final int to, final int n, final int keySize) {
      copyEntries(page, from, page, to, n, keySize);
   }

   /**
    * Copies the entries {@code [srcPos .. srcPos+n-1]}  of page {@code src} to positions
    * {@code [destPos .. destPos+n-1]} of page {@code dest}.
    * @param src source page
    * @param srcPos position in the source page
    * @param dest destination page
    * @param destPos position in the destination page
    * @param n number of entries to copy
    * @param keySize size of the key
    */
   static void copyEntries(final Page<BTreeLeaf> src, final int srcPos,
         final Page<BTreeLeaf> dest, final int destPos, final int n, final int keySize) {
      System.arraycopy(src.getData(), keyOffset(srcPos, keySize), dest.getData(), keyOffset(destPos, keySize),
            n * (keySize + RecordID.BYTES));
   }

   /**
    * Returns the offset of the key at the given position.
    * @param pos key position
    * @param keySize size of the key
    * @return offset of the key
    */
   private static int keyOffset(final int pos, final int keySize) {
      return pos * (keySize + RecordID.BYTES);
   }

   /**
    * Returns the offset of the record ID at the given position.
    * @param pos record ID position
    * @param keySize size of the key
    * @return offset of the record ID
    */
   private static int recordIDOffset(final int pos, final int keySize) {
      return pos * (keySize + RecordID.BYTES) + keySize;
   }
}

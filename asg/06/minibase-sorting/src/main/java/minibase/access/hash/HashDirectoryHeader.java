/*
 * @(#)HashDirectoryHeader.java   1.0   Sep 17, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import java.util.Arrays;

import minibase.RecordID;
import minibase.SearchKeyType;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.util.Convert;

/**
 * The header page of a {@link minibase.access.hash.LinearHashIndex}. It contains meta-data about the index's
 * configuration and the directory page ID. The header page always contains the first entries of the
 * directory. All following entries are then located on the next directory page.
 *
 * <pre>
 * ┌────────────────────┐
 * │HashDirectoryHeader │
 * ├────────────────────┴────────────────────────────┬─────┬─────────┬─────────┬─────────┬───────┬─────────┬──────────┐
 * │                                     |KEY  |KEYTY│DATA_│MAX_LOAD_│MIN_LOAD_│  NUM_   │       │WRITABLE_│NEXT_PAGE_│
 * │                     data[]          |TYPE |PE_LE│ENTRY│ FACTOR  │ FACTOR  │ BUCKETS │ SIZE  │  SIZE   │ POINTER  │
 * │                                     |     |NGTH │_SIZE│         │         │         │       │         │          │
 * └─────────────────────────────────────────────────┴──▲──┴────▲────┴────▲────┴────▲────┴───▲───┴────▲────┴────▲─────┘
 *                                                      │       │         │         │        │        │         │
 *                                                      │ Float.BYTES     │  Integer.BYTES   │   Short.BYTES    │
 *                                                      │                 │                  │                  │
 *                                                Integer.BYTES     Float.BYTES       Integer.BYTES        PageID.SIZE
 * </pre>
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni.kn&gt;
 * @version 1.0
 */
final class HashDirectoryHeader extends HashDirectoryPage {

   /** Offset of the size of the index. */
   private static final int SIZE_OFFSET = WRITABLE_SIZE_OFFSET - Integer.BYTES;
   /** Offset to the number of buckets in the index. */
   private static final int NUM_BUCKETS_OFFSET = SIZE_OFFSET - Integer.BYTES;
   /** Offset to the minimum load factor of the index. */
   private static final int MIN_LOAD_FACTOR_OFFSET = NUM_BUCKETS_OFFSET - Float.BYTES;
   /** Offset to the maximum load factor of the index. */
   private static final int MAX_LOAD_FACTOR_OFFSET = MIN_LOAD_FACTOR_OFFSET - Float.BYTES;
   /** Offset to the size of a data entry. */
   private static final int DATA_ENTRY_SIZE_OFFSET = MAX_LOAD_FACTOR_OFFSET - Integer.BYTES;
   /** Offset to the length of the key type. */
   private static final int KEYTYPE_LENGTH_OFFSET = DATA_ENTRY_SIZE_OFFSET - Short.BYTES;

   /** Hidden default constructor. */
   private HashDirectoryHeader() {
      super();
   }

   /**
    * Initializes a newly allocated page as a header page.
    *
    * @param bufferManager
    *           the buffer manager
    * @param numBuckets
    *           the number of buckets
    * @param minLoadFactor
    *           the minimum load factor
    * @param maxLoadFactor
    *           the maximum load factor
    * @param type
    *           the type of the search keys
    * @return the initialized header page
    */
   static Page<HashDirectoryHeader> newPage(final BufferManager bufferManager, final int numBuckets,
         final float minLoadFactor, final float maxLoadFactor, final SearchKeyType type) {
      @SuppressWarnings("unchecked")
      final Page<HashDirectoryHeader> header = (Page<HashDirectoryHeader>) bufferManager.newPage();
      return initPage(header, numBuckets, minLoadFactor, maxLoadFactor, type);
   }

   /**
    * Initializes a page as a header page.
    *
    * @param header
    *           the header page
    * @param numBuckets
    *           the number of buckets
    * @param minLoadFactor
    *           the minimum load factor
    * @param maxLoadFactor
    *           the maximum load factor
    * @param type
    *           the type of the search keys
    * @return the initialized header page
    */
   static Page<HashDirectoryHeader> initPage(final Page<HashDirectoryHeader> header, final int numBuckets,
         final float minLoadFactor, final float maxLoadFactor, final SearchKeyType type) {
      setNextPagePointer(header, PageID.INVALID);
      setSize(header, 0);
      setNumberBuckets(header, numBuckets);
      setMinLoadFactor(header, minLoadFactor);
      setMaxLoadFactor(header, maxLoadFactor);
      setEntrySize(header, type.getKeyLength() + RecordID.BYTES);
      setKeyType(header, type);
      header.writeShort(WRITABLE_SIZE_OFFSET, (short) getKeyTypeOffset(type));
      Arrays.fill(header.getData(), 0, getKeyTypeOffset(type), (byte) 0xff);
      return header;
   }

   /**
    * @param type the type of the keys
    * @return the size of available space
    */
   static int getWritableSize(final SearchKeyType type) {
      return getKeyTypeOffset(type) - Short.BYTES;
   }

   /**
    * @param page
    *           the header page of the index
    * @return the current size of the index
    */
   static int getSize(final Page<HashDirectoryHeader> page) {
      return page.readInt(SIZE_OFFSET);
   }

   /**
    * @param page
    *           the header page of the index
    * @param size
    *           the size
    */
   static void setSize(final Page<HashDirectoryHeader> page, final int size) {
      page.writeInt(SIZE_OFFSET, size);
   }

   /**
    * @param page
    *           the header page of the index
    * @return the number of buckets in the index
    */
   static int getNumberBuckets(final Page<HashDirectoryHeader> page) {
      return page.readInt(NUM_BUCKETS_OFFSET);
   }

   /**
    * @param page
    *           the header page of the index
    * @param num
    *           the number of buckets in the index
    */
   static void setNumberBuckets(final Page<HashDirectoryHeader> page, final int num) {
      page.writeInt(NUM_BUCKETS_OFFSET, num);
   }

   /**
    * @param page
    *           the header page of the index
    * @return the number of buckets in the index
    */
   static int getEntrySize(final Page<HashDirectoryHeader> page) {
      return page.readInt(DATA_ENTRY_SIZE_OFFSET);
   }

   /**
    * @param page
    *           the header page of the index
    * @param num
    *           the number of buckets in the index
    */
   static void setEntrySize(final Page<HashDirectoryHeader> page, final int num) {
      page.writeInt(DATA_ENTRY_SIZE_OFFSET, num);
   }

   /**
    * @param page
    *           the header page of the index
    * @return the type of the keys in the index
    */
   static SearchKeyType getKeyType(final Page<HashDirectoryHeader> page) {
      final byte[] data = page.getData();
      final short length = Convert.readShort(data, KEYTYPE_LENGTH_OFFSET);
      return SearchKeyType.readFrom(data, KEYTYPE_LENGTH_OFFSET - length);
   }

   /**
    * @param page
    *           the header page of the index
    * @param type
    *           the type of the search keys
    */
   static void setKeyType(final Page<HashDirectoryHeader> page, final SearchKeyType type) {
      type.writeTo(page.getData(), getKeyTypeOffset(type));
      Convert.writeShort(page.getData(), KEYTYPE_LENGTH_OFFSET, (short) type.getLength());
   }

   /**
    * @param type the type of the keys
    * @return the calculated offset for the type
    */
   private static int getKeyTypeOffset(final SearchKeyType type) {
      return KEYTYPE_LENGTH_OFFSET - type.getLength();
   }

   /**
    * @param page
    *           the header page of the index
    * @return the minimum load factor for the index
    */
   static float getMinLoadFactor(final Page<HashDirectoryHeader> page) {
      return page.readFloat(MIN_LOAD_FACTOR_OFFSET);
   }

   /**
    * @param page
    *           the header page of the index
    * @param min
    *           the minimum load factor of the index
    */
   static void setMinLoadFactor(final Page<HashDirectoryHeader> page, final float min) {
      page.writeFloat(MIN_LOAD_FACTOR_OFFSET, min);
   }

   /**
    * @param page
    *           the header page of the index
    * @return the maximum load factor of the index
    */
   static float getMaxLoadFactor(final Page<HashDirectoryHeader> page) {
      return page.readFloat(MAX_LOAD_FACTOR_OFFSET);
   }

   /**
    * @param page
    *           the header page of the index
    * @param max
    *           the maximum load factor of the index
    */
   static void setMaxLoadFactor(final Page<HashDirectoryHeader> page, final float max) {
      page.writeFloat(MAX_LOAD_FACTOR_OFFSET, max);
   }
}

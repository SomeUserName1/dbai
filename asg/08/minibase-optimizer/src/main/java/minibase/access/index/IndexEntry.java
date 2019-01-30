/*
 * @(#)IndexEntry.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.index;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;

/**
 * Records stored in an index file; using the textbook's "Alternative 2" (see page 276) to allow for multiple
 * indexes. Duplicate keys result in duplicate DataEntry instances. DataEntry instances are of fixed-size,
 * though they might contain String keys of variable size.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public final class IndexEntry {

   /** The search key (i.e. integer, float, string). */
   private final SearchKey searchKey;

   /** The data record (i.e. in some heap file). */
   private final RecordID recordID;

   /** The fixed length of a data entry. */
   private final SearchKeyType keyType;

   /**
    * Constructs a new data entry from the given key and record identifier using the given amount of space.
    *
    * @param key
    *           the search key
    * @param rid
    *           the record identifier
    * @param type
    *           the type of the search keys
    */
   public IndexEntry(final SearchKey key, final RecordID rid, final SearchKeyType type) {
      this.keyType = type;
      this.searchKey = key;
      this.recordID = rid;
   }

   /**
    * Getter for this entry's search key.
    *
    * @return search key
    */
   public SearchKey getSearchKey() {
      return this.searchKey;
   }

   /**
    * Returns the record identifier of this data entry.
    *
    * @return the record identifier
    */
   public RecordID getRecordID() {
      return this.recordID;
   }

   /**
    * Returns the actual length of the entry in compact form. <em>Note:</em> Do not use this method if you are
    * using the {@link #fromBuffer(byte[], int, int)} and {@link IndexEntry#writeData(byte[], int)} methods.
    *
    * @return the actual (compact) length of the entry
    */
   @Deprecated
   // TODO this method needs to go, once SortedPage is no longer used
   public short getActualLength() {
      return (short) (this.searchKey.getLength() + RecordID.BYTES);
   }

   /**
    * Returns the total length of the data entry in bytes.
    *
    * @return total length in bytes
    */
   public short getEntrySize() {
      return (short) (this.keyType.getKeyLength() + RecordID.BYTES);
   }

   /**
    * Returns the total length of a data entry in bytes, given the maximum search key length.
    *
    * @param maxSearchKeyFieldSize maximum search key length
    * @return total length in bytes
    */
   public static int getLength(final int maxSearchKeyFieldSize) {
      return maxSearchKeyFieldSize + RecordID.BYTES;
   }

   @Override
   public int hashCode() {
      return 31 * this.searchKey.hashCode() + this.recordID.hashCode();
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj instanceof IndexEntry) {
         final IndexEntry other = (IndexEntry) obj;
         return this.recordID.equals(other.recordID) && this.searchKey.equals(other.searchKey);
      }
      return false;
   }

   @Override
   public String toString() {
      return "DataEntry{" + "key=" + this.searchKey.toString() + ", rid=" + this.recordID.toString() + "}";
   }

   /**
    * Reads a data entry from the given byte buffer starting at the given offset and having the given entry size.
    *
    * @param data byte buffer to read from
    * @param offset offset in byte to read at
    * @param type the type of the search keys
    * @return the data entry at the specified offset in the byte buffer
    */
   public static IndexEntry fromBuffer(final byte[] data, final int offset, final SearchKeyType type) {
      final SearchKey key = type.readSearchKey(data, offset);
      final RecordID rid = new RecordID(data, offset + type.getKeyLength());
      return new IndexEntry(key, rid, type);
   }

   /**
    * Reads the search key of the data entry at the given offset in the given byte array.
    *
    * @param data byte array to read from
    * @param offset byte offset of the entry
    * @param type the type of the search keys
    * @return the search key
    */
   public static SearchKey readKey(final byte[] data, final int offset, final SearchKeyType type) {
      return type.readSearchKey(data, offset);
   }

   /**
    * Writes the data entry to the given byte buffer at the specified offset.
    *
    * @param data
    *           byte buffer to write to
    * @param offset
    *           offset to write at
    */
   public void writeData(final byte[] data, final int offset) {
      this.searchKey.writeRawData(data, offset);
      this.recordID.writeData(data, offset + this.keyType.getKeyLength());
   }

   /**
    * @return the type of the keys
    */
   public SearchKeyType getKeyType() {
      return this.keyType;
   }
}

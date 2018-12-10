/*
 * @(#)DataEntry.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.index;

import minibase.RecordID;
import minibase.SearchKey;

/**
 * Records stored in an index file; using the textbook's "Alternative 2" (see page 276) to allow for multiple
 * indexes. Duplicate keys result in duplicate DataEntry instances. DataEntry instances are of fixed-size,
 * though they might contain String keys of variable size.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public final class DataEntry {

   /** The search key (i.e. integer, float, string). */
   private final SearchKey searchKey;

   /** The data record (i.e. in some heap file). */
   private final RecordID recordID;

   /** The fixed length of a data entry. */
   private final int entrySize;

   /**
    * Constructs a new data entry from the given key and record identifier using the given amount of space.
    *
    * @param key
    *           the search key
    * @param rid
    *           the record identifier
    * @param entrySize
    *           size of the data entry
    */
   public DataEntry(final SearchKey key, final RecordID rid, final int entrySize) {
      this.entrySize = entrySize;
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
    * using the {@link #fromBuffer(byte[], int, int)} and {@link DataEntry#writeData(byte[], int)} methods.
    *
    * @return the actual (compact) length of the entry
    */
   @Deprecated
   // TODO this method needs to go, once SortedPage is no longer used
   public short getActualLength() {
      return (short) (this.searchKey.getLength() + RecordID.SIZE);
   }

   /**
    * Returns the total length of the data entry in bytes.
    *
    * @return total length in bytes
    */
   public short getEntrySize() {
      return (short) this.entrySize;
   }

   /**
    * Returns the total length of a data entry in bytes, given the maximum search key length.
    *
    * @param maxSearchKeyFieldSize maximum search key length
    * @return total length in bytes
    */
   public static int getLength(final int maxSearchKeyFieldSize) {
      return maxSearchKeyFieldSize + RecordID.SIZE;
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
      if (obj instanceof DataEntry) {
         final DataEntry other = (DataEntry) obj;
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
    * @param entrySize entry size in bytes to read
    * @return the data entry at the specified offset in the byte buffer
    */
   public static DataEntry fromBuffer(final byte[] data, final int offset, final int entrySize) {
      final SearchKey key = new SearchKey(data, offset);
      final RecordID rid = new RecordID(data, offset + entrySize - RecordID.SIZE);
      return new DataEntry(key, rid, entrySize);
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
      this.searchKey.writeData(data, offset);
      this.recordID.writeData(data, offset + this.entrySize - RecordID.SIZE);
   }
}

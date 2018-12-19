/*
 * @(#)RecordID.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import minibase.storage.buffer.PageID;
import minibase.util.Convert;

/**
 * A record is uniquely identified by its page number and slot number.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class RecordID {

   /** Size of a Record ID in bytes. */
   public static final int BYTES = PageID.BYTES + Integer.BYTES;

   /** The record's page number. */
   private final PageID pageID;

   /** The record's slot number. */
   private final int slotNo;

   /**
    * Constructs an RID from the given values.
    *
    * @param pageID
    *           the page ID
    * @param slotNo
    *           the slot number
    */
   public RecordID(final PageID pageID, final int slotNo) {
      this.pageID = pageID;
      this.slotNo = slotNo;
   }

   /**
    * Constructs an RID stored in the given data buffer.
    *
    * @param data
    *           page data buffer
    * @param offset
    *           data offset
    */
   public RecordID(final byte[] data, final int offset) {
      this.pageID = PageID.getInstance(Convert.readInt(data, offset));
      this.slotNo = Convert.readInt(data, offset + 4);
   }

   /**
    * Getter for the record's page ID.
    *
    * @return page ID
    */
   public PageID getPageID() {
      return this.pageID;
   }

   /**
    * Getter for the record's slot number.
    *
    * @return slot number
    */
   public int getSlotNo() {
      return this.slotNo;
   }

   /**
    * Writes the RID into the given data buffer.
    *
    * @param data
    *           data buffer
    * @param offset
    *           write offset
    */
   public void writeData(final byte[] data, final int offset) {
      Convert.writeInt(data, offset, this.pageID.getValue());
      Convert.writeInt(data, offset + 4, this.slotNo);
   }

   @Override
   public int hashCode() {
      return 31 * this.pageID.getValue() + this.slotNo;
   }

   @Override
   public boolean equals(final Object obj) {
      if (obj instanceof RecordID) {
         final RecordID rid = (RecordID) obj;
         return this.pageID.getValue() == rid.pageID.getValue()
               && this.slotNo == rid.slotNo;
      }
      return false;
   }

   @Override
   public String toString() {
      return this.pageID.toString() + ":" + Integer.toString(this.slotNo);
   }
}

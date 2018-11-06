/*
 * @(#)Page.java   1.0   Aug 2, 2006
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
import minibase.util.Convert;

/**
 * Lowest-level view of a disk page. It is parameterized by a phantom type that is used to attach static
 * type information to a page without incurring allocation overhead.
 * <p>
 * Methods for accessing contents of pages of a specific type (e.g. a <i>heap file page</i>) are normally
 * static and are gathered in a utility class <i>XYPage</i> that implements the {@link PageType} interface.
 * After a raw {@code Page<?>} is initialized for usage as this page type, it is cast to {@code Page<XYPage>}
 * and all static methods only accept {@code Page<XYPage>} objects.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 *
 * @param <T> the page's type
 */
public final class Page<T extends PageType> {
   /** Index of this page in the buffer pool. */
   private final int index;

   /** The actual byte array for the page. */
   private final byte[] data;

   /** Current page number of this page. */
   private PageID pageID = PageID.INVALID;

   /** Pin count of this page. */
   private int pinCount = 0;

   /** Dirty status of this page. */
   private boolean dirty = false;

   /**
    * Constructor.
    *
    * @param index index of this page in the buffer pool
    */
   public Page(final int index) {
      this.index = index;
      this.data = new byte[DiskManager.PAGE_SIZE];
   }

   /**
    * Returns the page ID of the page represented by this frame.
    *
    * @return The page ID of the represented page.
    */
   public PageID getPageID() {
      return this.pageID;
   }

   /**
    * Getter for the byte buffer.
    * @return the byte buffer
    */
   public byte[] getData() {
      return this.data;
   }

   /**
    * Gets a char at the given page offset.
    *
    * @param offset read offset
    * @return the char value
    */
   public char readChar(final int offset) {
      return Convert.readChar(this.data, offset);
   }

   /**
    * Sets a char at the given page offset.
    * @param offset write offset
    * @param value value to write
    */
   public void writeChar(final int offset, final char value) {
      Convert.writeChar(this.data, offset, value);
   }

   /**
    * Gets a short at the given page offset.
    *
    * @param offset read offset
    * @return the short value
    */
   public short readShort(final int offset) {
      return Convert.readShort(this.data, offset);
   }

   /**
    * Sets a short at the given page offset.
    * @param offset write offset
    * @param value short value to write
    */
   public void writeShort(final int offset, final short value) {
      Convert.writeShort(this.data, offset, value);
   }

   /**
    * Gets an int at the given page offset.
    *
    * @param offset read offset
    * @return the read value
    */
   public int readInt(final int offset) {
      return Convert.readInt(this.data, offset);
   }

   /**
    * Sets an int at the given page offset.
    * @param offset write offset
    * @param value the int value to write
    */
   public void writeInt(final int offset, final int value) {
      Convert.writeInt(this.data, offset, value);
   }

   /**
    * Gets a float at the given page offset.
    *
    * @param offset read offset
    * @return the read value
    */
   public float readFloat(final int offset) {
      return Convert.readFloat(this.data, offset);
   }

   /**
    * Sets a float at the given page offset.
    * @param offset write offset
    * @param value the float value to write
    */
   public void writeFloat(final int offset, final float value) {
      Convert.writeFloat(this.data, offset, value);
   }

   /**
    * Gets a string at the given page offset, given the maximum length.
    *
    * @param offset read offset
    * @param length length of the string to read
    * @return the string
    */
   public String readString(final int offset, final int length) {
      return Convert.readString(this.data, offset, length);
   }

   /**
    * Sets a string at the given page offset.
    * @param offset write offset
    * @param value the string value to write
    * @param maxLen maximum length of the string on disk
    */
   public void writeString(final int offset, final String value, final int maxLen) {
      Convert.writeString(this.data, offset, value, maxLen);
   }

   /**
    * Gets a page ID at the given offset.
    * @param offset read offset
    * @return the page ID
    */
   public PageID readPageID(final int offset) {
      return PageID.getInstance(Convert.readInt(this.data, offset));
   }

   /**
    * Sets the given page ID at the given offset in this page.
    * @param offset write offset
    * @param value page ID to write
    */
   public void writePageID(final int offset, final PageID value) {
      Convert.writeInt(this.data, offset, value.getValue());
   }

   /**
    * Gets the index of this page in the buffer pool.
    * @return index of this page in the buffer pool
    */
   int getIndex() {
      return this.index;
   }

   /**
    * Returns the pin count of this frame.
    *
    * @return The pin count.
    */
   int getPinCount() {
      return this.pinCount;
   }

   /**
    * Increments the pin count of this frame descriptor.
    */
   void incrementPinCount() {
      this.pinCount++;
   }

   /**
    * Decrements the pin count of this frame descriptor.
    */
   void decrementPinCount() {
      this.pinCount--;
   }

   /**
    * Queries whether this page is dirty.
    *
    * @return {@code true} if the page is dirty, {@code false} otherwise
    */
   boolean isDirty() {
      return this.dirty;
   }

   /**
    * Sets the dirty status of this page.
    *
    * @param dirty
    *           {@code true} if the page is dirty, {@code false} otherwise
    */
   void setDirty(final boolean dirty) {
      this.dirty = dirty;
   }

   /**
    * Resets this page to represent the disk block with the given ID.
    * This resets the pin count to {@code 0} and the page's status to clean.
    *
    * @param pageID
    *           the ID of the represented page
    */
   void reset(final PageID pageID) {
      this.pageID = pageID;
      this.pinCount = 0;
      this.dirty = false;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("Page#").append(this.getPageID()).append('[');
      for (int i = 0; i < this.data.length / 32; i++) {
         sb.append("\n   ");
         final int thisLine = Math.min(this.data.length - i * 32, 32);
         for (int j = 0; j < thisLine / 4; j++) {
            sb.append(String.format(" %08x", this.readInt(i * 32 + j * 4)));
         }
      }
      return sb.append(']').toString();
   }
}

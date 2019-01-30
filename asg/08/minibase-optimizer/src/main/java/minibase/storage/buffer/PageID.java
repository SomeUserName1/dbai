/*
 * @(#)PageID.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

/**
 * Although a PageID is simply an integer, wrapping it provides convenience methods.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class PageID {
   /** The invalid page ID (with value {@code -1}). */
   public static final PageID INVALID = new PageID(-1);
   /** Number of bytes used to represent a PageID. */
   public static final int BYTES = Integer.BYTES;

   /** The actual page id value. */
   private final int value;

   /**
    * Constructs a PageID from the given value.
    * @param id the page ID
    */
   private PageID(final int id) {
      this.value = id;
   }

   /**
    * Returns a PageID with the given value.
    * @param value the value
    * @return the PageID
    */
   public static PageID getInstance(final int value) {
      return value == -1 ? INVALID : new PageID(value);
   }

   /**
    * Gets the integer value of this page ID.
    * @return the ID value
    */
   public int getValue() {
      return this.value;
   }

   /**
    * Checks if this page ID is valid (i.e. it is not equal to {@link PageID#INVALID}).
    * @return {@code true} if this page ID is valid, {@code false} otherwise
    */
   public boolean isValid() {
      return !PageID.INVALID.equals(this);
   }

   @Override
   public int hashCode() {
      return this.value;
   }

   @Override
   public boolean equals(final Object obj) {
      return this == obj || obj instanceof PageID && ((PageID) obj).value == this.value;
   }

   @Override
   public String toString() {
      return Integer.toString(this.value);
   }
}

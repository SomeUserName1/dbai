/*
 * @(#)Run.java   1.0   Jul 13, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import minibase.storage.buffer.PageID;

/**
 * A run is a sequence of records stored on a linked list of {@link RunPage}s.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class Run {
   /** Page ID of the first page in this run. */
   private final PageID firstPageID;
   /** Number of records in this run. */
   private final long length;

   /**
    * Create a run with the given first page ID and number of records.
    *
    * @param firstPageID page ID of the run's first page
    * @param length number of records in this run
    */
   public Run(final PageID firstPageID, final long length) {
      this.firstPageID = firstPageID;
      this.length = length;
   }

   /**
    * Returns the page ID of this run's first page.
    *
    * @return ID of the first run page of this run
    */
   public PageID getFirstPageID() {
      return this.firstPageID;
   }

   /**
    * Returns the number of records in this run.
    *
    * @return number of records
    */
   public long getLength() {
      return this.length;
   }
}

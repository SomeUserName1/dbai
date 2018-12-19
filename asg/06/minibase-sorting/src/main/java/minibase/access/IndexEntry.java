/*
 * @(#)IndexEntry.java   1.0   Dec 02, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access;

import minibase.RecordID;

/**
 * The entry of an unclustered index, i.e. the pair of a search key (here only integers) and a record ID.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public class IndexEntry {

   /** Key of this index entry. */
   private final int key;
   
   /** Record ID of this index entry. */
   private final RecordID rid;

   /**
    * Creates an index entry with the given key and record ID.
    *
    * @param key search key
    * @param recordID record ID
    */
   public IndexEntry(final int key, final RecordID recordID) {
      this.key = key;
      this.rid = recordID;
   }

   /**
    * Returns this index entry's search key.
    *
    * @return search key
    */
   public int getKey() {
      return this.key;
   }

   /**
    * Returns this index entry's record ID.
    *
    * @return record ID
    */
   public RecordID getRecordID() {
      return this.rid;
   }
}

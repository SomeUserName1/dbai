/*
 * @(#)HashIndex.java   1.0   Dec 2, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */

package minibase.access.hash;

import java.util.Iterator;

import minibase.SearchKey;
import minibase.access.index.Index;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.PageID;

/**
 * Interface for hash based indices.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 * @version 1.0
 */
public interface HashIndex extends Index {

   /**
    * Returns the underlying BufferManager.
    *
    * @return the BufferManager
    */
   @Deprecated
   BufferManager getBufferManager();

   /**
    * Returns the global depth of the index.
    *
    * @return global depth
    */
   @Deprecated
   int getDepth();

   /**
    * Returns the first page of the hash directory.
    *
    * @return first directory page
    */
   @Deprecated
   PageID getHeadID();

   /**
    * @return iterator over all directory entries.
    */
   Iterator<PageID> directoryIterator();

   /**
    * @param key
    *           Key
    * @return scan
    */
   @Override
   IndexScan openScan(SearchKey key);
   /**
    * @return scan
    */
   @Override
   HashIndexScan openScan();

   /**
    * Prints a summary of the index.
    */
   void printSummary();
}

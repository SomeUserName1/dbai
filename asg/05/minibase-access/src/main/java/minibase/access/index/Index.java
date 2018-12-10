/*
 * @(#)Index.java   1.0   Nov 21, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import minibase.RecordID;
import minibase.SearchKey;

/**
 * An interface for an index.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
 * @version 1.0
 */
public interface Index extends Closeable {

   /**
    * Deletes the index file from the database, freeing all of its pages.
    */
   void delete();

   /**
    * Searches the index for the given key and returns the first match.
    *
    * @param key the search key of the entry to retrieve
    * @return the first match with the search key, {@link Optional#empty()} if no match
    * @throws IOException I/O exception
    */
   Optional<DataEntry> search(SearchKey key) throws IOException;

   /**
    * <p>Inserts a new entry into the index.</p> <p><b>Note:</b> Inserting the same entry twice, breaks the index.
    * So be sure that the entry is not already contained in the index.</p>
    *
    * @param key
    *           the search key of the new entry
    * @param rid
    *           the record identifier of the new entry
    * @throws IllegalArgumentException
    *            if the entry is too large
    */
   void insert(SearchKey key, RecordID rid);

   /**
    * Removes the specified entry from the index file.
    *
    * @param key
    *           the search key of the entry to be deleted
    * @param rid
    *           the record identifier of the entry to be deleted (to handle duplicates)
    * @return {@code true} if the entry was deleted, {@code false} otherwise
    */
   boolean remove(SearchKey key, RecordID rid);

   /**
    * Initiates a scan of the entire index.
    *
    * @return an index scan
    */
   IndexScan openScan();

   /**
    * Initiates an equality scan of the index based on the give search key.
    *
    * @param key
    *           the search key to scan for
    * @return an index scan
    */
   IndexScan openScan(SearchKey key);
}

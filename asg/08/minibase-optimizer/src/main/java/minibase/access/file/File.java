/*
 * @(#)File.java   1.0   Dec 19, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import java.util.Optional;

import minibase.RecordID;
import minibase.query.evaluator.TupleIterator;

/**
 * Common interface for all database file implementations.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public interface File extends AutoCloseable {

   /**
    * Inserts a new record into the file and returns its record ID.
    *
    * @param record
    *           the record data
    * @return the record's ID
    * @throws IllegalArgumentException
    *            if the record is too large
    */
   RecordID insertRecord(byte[] record);

   /**
    * Reads a record from the file, given its record ID.
    *
    * @param rid
    *           ID of the record to read
    * @return the data
    * @throws IllegalArgumentException
    *            if the rid is invalid
    */
   byte[] selectRecord(RecordID rid);

   /**
    * Updates the specified record in the heap file. This operation is optional and may throw
    * an exception in the case of read-only files.
    *
    * @param rid
    *           ID of the record to update
    * @param newRecord
    *           new data of the record
    * @throws IllegalArgumentException
    *            if the rid or new record is invalid
    * @throws UnsupportedOperationException
    *            if the file is read-only
    */
   void updateRecord(RecordID rid, byte[] newRecord);

   /**
    * Deletes the specified record from the heap file. This operation is optional and may
    * throw an exception in the case of read-only files.
    *
    * @param rid
    *           ID of the record to delete
    * @throws IllegalArgumentException
    *            if the rid is invalid
    * @throws UnsupportedOperationException
    *            if the file is read-only
    */
   void deleteRecord(RecordID rid);

   /**
    * Initiates a sequential scan of the file.
    *
    * @return file scan
    */
   FileScan openScan();

   /**
    * Bulk-load the file from the given {@link TupleIterator}.
    *
    * @param records iterator over records
    * @param recordLength length of a record
    * @return number of loaded records
    */
   int bulkLoad(TupleIterator records, int recordLength);

   /**
    * @return file name, or {@link Optional#empty()} for anonymous file
    */
   Optional<String> getName();

   @Override
   void close();

   /**
    * Deletes the file from the database, freeing all of its pages.
    *
    * Note: non-temporary files can explicitly be deleted prior to closing.
    */
   void delete();
}

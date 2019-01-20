/*
 * @(#)HeapFileTest.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import minibase.BaseTest;
import minibase.RecordID;
import minibase.TestHelper;
import minibase.storage.buffer.BufferManager;
import minibase.storage.file.DiskManager;
import minibase.util.Convert;

/**
 * Test suite for the heap layer.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 */
public class HeapFileTest extends BaseTest {

   /** Size of heap file to create in test cases. */
   private static final int FILE_SIZE = 100_000;

   /** Heap file for tests. */
   private HeapFile file;

   /** Creates the test file. */
   @Before
   public void createFile() {
      final BufferManager bufferManager = this.getBufferManager();
      final int numPinned = bufferManager.getNumPinned();

      // Create a heap file
      final HeapFile f = HeapFile.createTemporary(bufferManager);
      assertEquals(numPinned, bufferManager.getNumPinned());

      // Add HeapFileTest.FILE_SIZE records to the file
      for (int i = 0; i < HeapFileTest.FILE_SIZE; i++) {
         // fixed length record
         final DummyRecord rec = new DummyRecord(i, (float) (i * 2.5), "record" + i);

         f.insertRecord(rec.toByteArray());
         assertEquals(numPinned, bufferManager.getNumPinned());
      }
      this.file = f;

      assertEquals(HeapFileTest.FILE_SIZE, f.getRecordCount());
   }

   /** Deletes the test file. */
   @After
   public void deleteFile() {
      this.file.delete();
   }

   /** Insert and scan fixed-size records. */
   @Test
   public void insertScan() {
      final BufferManager bufferManager = this.getBufferManager();
      final int numPinned = bufferManager.getNumPinned();
      // In general, a sequential scan won't be in the same order as the
      // insertions. However, we're inserting fixed-length records here, and
      // in this case the scan must return the insertion order.

      // Scan the records just inserted
      int i = 0;
      try (FileScan scan = this.file.openScan()) {
         // The heap-file scan should pin the first page
         assertNotEquals(0, bufferManager.getNumPinned());

         for (; scan.hasNext(); i++) {
            final byte[] tuple = scan.next();
            final DummyRecord rec = new DummyRecord(tuple);
            assertEquals(tuple.length, rec.length());

            // The heap-file scan should leave its page pinned
            assertNotEquals(numPinned, bufferManager.getNumPinned());

            assertEquals(i, rec.ival);
            assertEquals(i * 2.5f, rec.fval, 0);
            assertEquals("record" + i, rec.name);
         }
         // If it gets here, then the scan should be completed
      }

      assertEquals(numPinned, bufferManager.getNumPinned());
      assertEquals(HeapFileTest.FILE_SIZE, i);
   }

   /** Delete fixed-size records. */
   @Test
   public void delete() {
      final BufferManager bufferManager = this.getBufferManager();
      final int numPinned = bufferManager.getNumPinned();

      // Open the same heap file as test 1
      try (FileScan scan = this.file.openScan()) {
         // Delete half the records
         for (int i = 0; scan.hasNext(); i++) {
            scan.next();

            // Delete the odd-numbered ones.
            if (i % 2 == 1) {
               this.file.deleteRecord(scan.lastID());
            }
         }
      }

      assertEquals(numPinned, bufferManager.getNumPinned());

      // Scan the remaining records
      try (FileScan scan2 = this.file.openScan()) {
         for (int i = 0; scan2.hasNext(); i += 2) {
            final byte[] tuple = scan2.next();
            final DummyRecord rec = new DummyRecord(tuple);
            assertEquals(i, rec.ival);
            assertEquals(i * 2.5f, rec.fval, 0);
         }
      }
   }

   /** Update fixed-size records. */
   @Test
   public void update() {
      this.delete();
      final BufferManager bufferManager = this.getBufferManager();
      final int numPinned = bufferManager.getNumPinned();

      // Open the same heap file as tests 1 and 2
      // Change the records
      try (FileScan scan = this.file.openScan()) {
         for (int i = 0; scan.hasNext(); i += 2) {
            final byte[] tuple = scan.next();
            final DummyRecord rec = new DummyRecord(tuple);
            // We'll check that i==rec.ival below.
            rec.fval = (float) 7 * i;
            this.file.updateRecord(scan.lastID(), rec.toByteArray());
         }
      }

      assertEquals(numPinned, bufferManager.getNumPinned());

      // Check that the updates are really there
      try (FileScan scan = this.file.openScan()) {
         for (int i = 0; scan.hasNext(); i += 2) {
            final DummyRecord rec = new DummyRecord(scan.next());
            assertEquals(i, rec.ival);
            assertEquals(i * 7f, rec.fval, 0);

            // While we're at it, test the getRecord method too.
            final DummyRecord rec2 = new DummyRecord(this.file.selectRecord(scan.lastID()));
            assertEquals(i, rec2.ival);
            assertEquals(i * 7f, rec2.fval, 0);
         }
      }
   }

   /** Test some error conditions. */
   @Test
   public void errorConditions() {
      this.update();
      // Try to change the size of a record
      try (FileScan scan = this.file.openScan()) {
         // The following is to test whether tinkering with the size of
         // the tuples will cause any problem.

         final byte[] tuple = scan.next();
         final RecordID rid = scan.lastID();
         assertNotNull(tuple);

         final DummyRecord rec = new DummyRecord(tuple);
         rec.name = "short";
         final byte[] newTuple = rec.toByteArray();

         TestHelper.assertThrows(IllegalArgumentException.class, () -> this.file.updateRecord(rid, newTuple));

         final DummyRecord rec2 = new DummyRecord(tuple);
         rec2.name = "this one's longer!";
         final byte[] newTuple2 = rec2.toByteArray();

         TestHelper.assertThrows(IllegalArgumentException.class, () -> this.file.updateRecord(rid, newTuple2));
      }

      // Try to insert a record that's too long
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> this.file.insertRecord(new byte[DiskManager.PAGE_SIZE + 4]));
   }

   /**
    * Used in fixed-length record test cases.
    */
   private class DummyRecord {

      /** Integer value. */
      private final int ival;
      /** Float value. */
      private float fval;
      /** String value. */
      private String name;

      /**
       * Constructs with default values.
       *
       * @param ival
       *           integer value
       * @param fval
       *           float value
       * @param name
       *           string value
       */
      DummyRecord(final int ival, final float fval, final String name) {
         this.ival = ival;
         this.fval = fval;
         this.name = name;
      }

      /**
       * Constructs from a byte array.
       *
       * @param data
       *           data to read
       */
      DummyRecord(final byte[] data) {
         this.ival = Convert.readInt(data, 0);
         this.fval = Convert.readFloat(data, 4);
         this.name = Convert.readString(data, 8, DiskManager.NAME_MAXLEN);
      }

      /**
       * Gets a byte array representation.
       *
       * @return this record's values written to a byte array
       */
      byte[] toByteArray() {
         final byte[] data = new byte[this.length()];
         Convert.writeInt(data, 0, this.ival);
         Convert.writeFloat(data, 4, this.fval);
         Convert.writeString(data, 8, this.name, DiskManager.NAME_MAXLEN);
         return data;
      }

      /**
       * Gets the length of the record.
       *
       * @return this record's length
       */
      public int length() {
         return 4 + 4 + this.name.length();
      }
   }
}

/*
 * @(#)IndexTest.java   1.0   Jan 13, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;

import org.junit.Test;

import minibase.BaseTest;
import minibase.RecordID;
import minibase.SearchKey;
import minibase.TestHelper;
import minibase.access.hash.LinearHashIndex;
import minibase.access.hash.StaticHashIndex;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.PageID;
import minibase.storage.file.DiskManager;

/**
 * Base class for index tests, derived from the old StaticHashIndexTest.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
 */
public abstract class IndexTest extends BaseTest {

   /**
    * Size of index files to create in test cases.
    */
   private static final int FILE_SIZE = 9000;

   /**
    * Opens an existing index under test.
    *
    * @param fileName
    *           file name of the index under test
    * @param searchKeyLength
    *           maximum length of the search key
    * @return opened index
    */
   protected abstract Index openIndex(String fileName, int searchKeyLength);

   /**
    * Creates an index for testing.
    *
    * @param fileName
    *           file name of the index
    * @param searchKeyLength
    *           maximum length of the search key
    * @return created index
    */
   protected abstract Index createIndex(String fileName, int searchKeyLength);

   /**
    * Creates a temporary index.
    *
    * @param searchKeyLength
    *           maximum length of the search key
    * @return created temporary index
    */
   protected abstract Index createTempIndex(int searchKeyLength);

   /**
    * Simple use of temp index.
    *
    * @throws IOException
    *            I/O exception while closing
    */
   @Test
   public void simpleTempIndex() throws IOException {
      this.initCounts();
      this.saveCounts(null);
      this.initRandom();

      final int keyType = 1;

      final int searchKeyLength = this.randKey(keyType).getLength();

      // creating temp index...
      final Index temp = this.createTempIndex(searchKeyLength);
      final List<SearchKey> inserted = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
         final SearchKey key = this.randKey(keyType);
         final RecordID rid = new RecordID(PageID.getInstance(i), 0);
         temp.insert(key, rid);
         inserted.add(key);
         for (int j = 0; j <= i; j++) {
            final SearchKey searchedKey = inserted.get(j);
            final Optional<DataEntry> entry = temp.search(searchedKey);
            assertTrue("Entry " + j + " could not be found after inserting " + i + ".", entry.isPresent());
            final RecordID ridFound = entry.get().getRecordID();
            assertEquals(j, ridFound.getPageID().getValue());
         }
      }

      if (temp instanceof LinearHashIndex) {
         ((LinearHashIndex) temp).printSummary();
      }

      this.saveCounts("create");
      this.saveCounts(null);

      temp.delete();
      this.saveCounts("delete");
      this.printSummary(3);
   }

   /**
    * Tests the index with string keys.
    */
   @Test
   public void testStringKeys() {
      this.initCounts();
      this.initRandom();

      final int maxLength = 56;
      final Index index = this.createIndex("String_Test", maxLength);

      for (int i = 0; i < 20; ++i) {
         final SearchKey key = this.randKey(3);
         assertTrue(maxLength >= key.getLength());
         final RecordID rid = newRecordID(i);
         index.insert(key, rid);
      }

      this.saveCounts("ins");
      if (index instanceof LinearHashIndex) {
         ((LinearHashIndex) index).printSummary();
      }

      index.delete();
      this.saveCounts("drop");

      this.printSummary(4);
   }

   /**
    * StaticHashIndex under normal conditions.
    *
    * @throws IOException
    *            if the storage layer has problems
    */
   @Test
   public void normalConditions() throws IOException {
      // Larger, persistent hash indexes
      this.initCounts();

      for (int type = 1; type <= 3; type++) {
         // TODO MANUEL fix variable sized string search keys not working
         this.saveCounts(null);
         this.initRandom();
         final String fileName = "IX_Customers" + type;
         try (Index index = this.createIndex(fileName,
               type == 3 ? 56 : this.randKey(type).getLength())) {

            this.initRandom();
            for (int i = 0; i < IndexTest.FILE_SIZE; i++) {
               // insert a random entry
               final SearchKey key = this.randKey(type);
               final RecordID rid = new RecordID(PageID.getInstance(i), 0);
               index.insert(key, rid);
            }

            this.saveCounts("ins" + type);
            if (index instanceof LinearHashIndex) {
               ((LinearHashIndex) index).printSummary();
            }
            this.saveCounts(null);

            this.initRandom();
            // scanning every other entry...
            for (int i = 0; i < IndexTest.FILE_SIZE; i += 2) {
               // search for the random entry
               final SearchKey key = this.randKey(type);
               // to keep in sync with inserts
               this.randKey(type);
               final RecordID rid = new RecordID(PageID.getInstance(i), 0);
               boolean found = false;
               try (IndexScan scan = index.openScan(key)) {
                  while (scan.hasNext()) {
                     final RecordID rid2 = scan.next().getRecordID();
                     if (rid2.equals(rid)) {
                        found = true;
                     }
                  }
               }

               assertTrue("Search key " + key + " not found in scan! (" + i + ")", found);
            }

            this.saveCounts("scan" + type);
            this.saveCounts(null);
            this.initRandom();

            // deleting every other entry...
            for (int i = 0; i < IndexTest.FILE_SIZE; i += 2) {
               // delete the random entry
               final SearchKey key = this.randKey(type);
               // to keep in sync with inserts
               this.randKey(type);
               final RecordID rid = new RecordID(PageID.getInstance(i), 0);
               index.remove(key, rid);
            }

            this.saveCounts("del" + type);
            // index.printSummary();
         }
         this.saveCounts(null);

         // delete the file
         try (Index byebye = this.openIndex(fileName, this.randKey(type).getLength())) {
            byebye.delete();

            this.saveCounts("drop" + type);
         }
      }

      // made it without exceptions
      this.printSummary(4);
   }

   /**
    * Tiny fuzzy test.
    *
    * @throws IOException
    *            if something with the index file is wrong
    */
   @Test
   public void fuzzyTestTiny() throws IOException {
      this.fuzzyTest(100, 10);
   }

   /**
    * Small fuzzy test.
    *
    * @throws IOException
    *            if something with the index file is wrong
    */
   @Test
   public void fuzzyTestSmall() throws IOException {
      this.fuzzyTest(1000, 100);
   }

   /**
    * Normal fuzzy test.
    *
    * @throws IOException
    *            if something with the index file is wrong
    */
   @Test
   public void fuzzyTestNormal() throws IOException {
      this.fuzzyTest(100000, 10000);
   }

   /**
    * Big fuzzy test.
    *
    * @throws IOException
    *            if something with the index file is wrong
    */
   @Test
   public void fuzzyTestBig() throws IOException {
      this.fuzzyTest(500000, 10000);
   }

   /**
    * Executes the given number of random operations (insert, delete, lookup) with the given number of
    * distinct keys.
    *
    * @param ops
    *           number of operations
    * @param numKeys
    *           number of distinct keys
    * @throws IOException
    *            io exc
    */
   private void fuzzyTest(final int ops, final int numKeys) throws IOException {
      final BufferManager bufferManager = this.getBufferManager();
      final int pages = bufferManager.getDiskManager().getAllocCount();
      final String fileName = "fuzzyTest";
      final Random rand = new Random(ops);

      try (Index index = this.createIndex(fileName, this.randKey(1).getLength())) {
         final Map<SearchKey, List<RecordID>> map = new HashMap<>();

         final int oldCount = this.getBufferManager().getNumPinned();
         for (int i = 0; i < ops; i++) {
            if ((i + 1) % 10000 == 0) {
               System.out.println(i + 1 + " operations:");
               if (index instanceof StaticHashIndex) {
                  ((StaticHashIndex) index).printSummary();
               }
               if (index instanceof LinearHashIndex) {
                  ((LinearHashIndex) index).printSummary();
               }
            }

            final int keyInt = rand.nextInt(numKeys) - numKeys / 2;
            final SearchKey key = new SearchKey(keyInt);
            final int nextOp = rand.nextInt(30001);
            if (nextOp < 10000) {
               // insert value
               final RecordID rid = newRecordID(Math.abs(keyInt));
               map.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
               index.insert(key, rid);
               final DataEntry found = index.search(key).get();
               assertEquals(rid, found.getRecordID());
            } else if (nextOp < 20000) {
               // random key lookup
               final Optional<DataEntry> data = index.search(key);
               RecordID rid = null;
               if (data.isPresent()) {
                  rid = data.get().getRecordID();
                  assertTrue(map.getOrDefault(key, new ArrayList<>()).contains(rid));
               }
            } else if (nextOp < 30000) {
               // delete key
               final RecordID rid = newRecordID(Math.abs(keyInt));

               // delete key,rid pair from map
               final List<RecordID> records = map.get(key);
               if (records != null) {
                  records.remove(rid);
                  if (records.size() == 0) {
                     map.remove(key);
                  }
                  // delete also from index
                  index.remove(key, rid);
               }

               int newNum = 0;
               try (IndexScan scan = index.openScan(key)) {
                  while (scan.hasNext()) {
                     newNum++;
                     scan.next();
                  }
               }
               if (records != null) {
                  assertTrue(records.size() == newNum);
               } else {
                  assertEquals(0, newNum);
               }
            } else {
               // check iterator, 1/10000 as likely as the other operations
               try (IndexScan fromKey = index.openScan(key)) {
                  final Iterator<RecordID> mapIter = map.getOrDefault(key, new ArrayList<>()).iterator();
                  while (fromKey.hasNext()) {
                     assertTrue(mapIter.hasNext());
                     assertEquals(mapIter.next(), fromKey.next().getRecordID());
                  }
                  assertFalse(mapIter.hasNext());
               }
            }

            // test pin counts
            assertTrue("i" + i + ", op: " + nextOp + ", key: " + keyInt,
                  oldCount == this.getBufferManager().getNumPinned());
         }
         index.delete();
      }
      assertEquals(pages, bufferManager.getDiskManager().getAllocCount());
   }

   /**
    * Tests filling and emptying the index.
    *
    * @throws IOException
    *            if the index could not be closed
    */
   @Test
   public void testFillEmpty() throws IOException {
      this.initRandom();
      final SearchKey randomKey = this.randKey(1);

      this.initRandom();
      try (Index index = this.createIndex("fill_empty", randomKey.getLength())) {

         for (int i = 0; i < 10; i++) {
            final SearchKey key = this.randKey(1);
            final RecordID rid = newRecordID(i);
            index.insert(key, rid);
            final Optional<DataEntry> entry = index.search(key);
            assertTrue("Entry #" + i + " was not found.", entry.isPresent());
         }

         if (index instanceof LinearHashIndex) {
            ((LinearHashIndex) index).printSummary();
         }

         this.initRandom();
         for (int i = 0; i < 10; i++) {
            final SearchKey key = this.randKey(1);
            final RecordID rid = newRecordID(i);
            assertTrue("Entry #" + i + " could not be deleted.", index.remove(key, rid));
         }

         index.delete();
      }
   }

   /**
    * Index package error cases.
    *
    * @throws IOException
    *            if the storage layer has problems
    */
   @Test
   public void errorConditions() throws IOException {
      this.initCounts();
      this.saveCounts(null);
      this.initRandom();

      // creating temporary index...
      try (Index index = this.createTempIndex(new SearchKey(1).getLength())) {

         this.saveCounts("create");
         this.saveCounts(null);

         // inserting invalid entry...
         SearchKey key = new SearchKey(1);
         final RecordID rid = new RecordID(PageID.INVALID, 0);
         try {
            index.insert(key, rid);
            fail();
         } catch (final IllegalArgumentException exc) {
            // expected
         }

         // TODO test for insertion of over-sized entry

         // deleting invalid entry...
         key = this.randKey(1);
         assertFalse(index.remove(key, rid));

         // next in completed scan...
         try (IndexScan scan = index.openScan(key)) {
            scan.hasNext();
            try {
               scan.next();
               fail();
            } catch (final NoSuchElementException exc) {
               // expected
            }
         }

         this.saveCounts("errors");
         this.saveCounts(null);

         // printing empty index...
         // ((StaticHashIndex) index).printSummary();

         this.saveCounts("print");
         this.saveCounts(null);

         // deleting empty index...
         index.delete();

         this.saveCounts("delete");
         this.printSummary(4);
      }
   }

   /**
    * Does random inserts and deletes and checks if the index leaves something pinned at the end.
    *
    * @throws IOException
    *            exception of index close
    */
   @Test
   public void testPincountsInsertDelete() throws IOException {
      // Do some random operations on the index
      final String fileName = "reopenTest";
      final BufferManager bufferManager = this.getBufferManager();
      final DiskManager diskManager = bufferManager.getDiskManager();

      final int allocStart = diskManager.getAllocCount();
      try (Index index = this.createIndex(fileName, new SearchKey(0).getLength())) {

         final int pinned = bufferManager.getNumPinned();
         final int alloc = diskManager.getAllocCount();

         final Map<SearchKey, List<RecordID>> map = new HashMap<>();
         final Random rand = new Random(42);
         for (int i = 0; i < 1382; ++i) {
            final int r = rand.nextInt(10000);
            final SearchKey key = new SearchKey(r);
            final RecordID rid = newRecordID(r);
            final int pinnedBeforeInsert = bufferManager.getNumPinned();
            index.insert(key, rid);
            assertEquals(pinnedBeforeInsert, bufferManager.getNumPinned());
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
         }
         // PinCount should stay the same after all operations
         assertTrue(pinned == bufferManager.getNumPinned());

         int rem = 0;
         for (final Entry<SearchKey, List<RecordID>> e : map.entrySet()) {
            for (final RecordID rid : e.getValue()) {
               final int pinnedBefore = bufferManager.getNumPinned();
               index.remove(e.getKey(), rid);
               assertEquals("Remove op " + rem, pinnedBefore, bufferManager.getNumPinned());
               rem++;
            }
         }

         assertEquals("Not all pages are unpinned after all operations.", pinned,
               bufferManager.getNumPinned());

         if (index instanceof LinearHashIndex) {
            assertEquals("Not all pages were removed by delete.", alloc, diskManager.getAllocCount());
         }

         index.delete();
         assertEquals("The index did not delete itself properly.", allocStart, diskManager.getAllocCount());
      }
   }

   /**
    * Tests {@code null} handling on insert.
    *
    * @throws IOException
    *            index close exception
    */
   @Test
   public void testNullInsert() throws IOException {
      try (Index index = this.createIndex("garbageTest", 4)) {
         TestHelper.assertThrows(NullPointerException.class, () -> index.insert(null, null));
         index.delete();
      }
   }

   /**
    * Tests {@code null} handling on remove.
    *
    * @throws IOException
    *            index close exception
    */
   @Test
   public void testNullRemove() throws IOException {
      try (Index index = this.createIndex("garbageTest", 4)) {
         TestHelper.assertThrows(NullPointerException.class, () -> index.remove(null, null));
         index.delete();
      }
   }

   /**
    * Tests {@code null} handling on search.
    *
    * @throws IOException
    *            I/O exception
    */
   @Test
   public void testNullSearch() throws IOException {
      try (Index index = this.createIndex("garbageTest", 4)) {
         try {
            index.search(null);
            fail();
         } catch (final NullPointerException e) {
            // expected
         }
         index.delete();
      }
   }

   /**
    * Tests reopening of the index.
    *
    * @throws Exception
    *            exception
    */
   @Test
   public void testReopen() throws Exception {
      // Do some random operations on the index
      final String fileName = "reopenTest";
      final int keyLength = new SearchKey(0).getLength();
      final Map<SearchKey, List<RecordID>> map = new HashMap<>();
      try (Index index = this.createIndex(fileName, keyLength)) {
         final Random rand = new Random(42);
         for (int i = 0; i < 10000; ++i) {
            final int r = rand.nextInt(10000);
            final SearchKey key = new SearchKey(r);
            final RecordID rid = newRecordID(r);
            index.insert(key, rid);
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
         }
      }
      // Reopen index
      System.out.println("Reopening index...");
      try (Index index = this.openIndex(fileName, keyLength)) {
         // Simply look for one unspecific entry
         index.search(map.keySet().iterator().next());
      }
   }

   /**
    * Tests the scan of the whole index.
    * 
    * @throws IOException
    *             exception
    */
   @Test
   public void scanIndex() throws IOException {
      final int size = 600;
      try (Index index = this.createTempIndex(new SearchKey(0).getLength())) {
         final BitSet bs = new BitSet(size);
         for (int i = 0; i < size; i++) {
            final SearchKey key = new SearchKey(i);
            final RecordID rid = newRecordID(i);
            index.insert(key, rid);
            bs.set(i);
         }
         try (IndexScan scan = index.openScan()) {
            while (scan.hasNext()) {
               final DataEntry e = scan.next();
               assertTrue(bs.get(e.getRecordID().getSlotNo()));
               bs.clear(e.getRecordID().getSlotNo());
            }
         }
         assertTrue(bs.isEmpty());
      }
   }
   
   /**
    * Gets a random search key, given the type.
    *
    * @param type
    *           search key type
    * @return a random search key
    */
   SearchKey randKey(final int type) {
      switch (type) {
         case 1:
            return new SearchKey(this.getRandom().nextInt());
         case 2:
            return new SearchKey(this.getRandom().nextFloat());
         case 3:
            return new SearchKey(this.randEmail());
         default:
            throw new IllegalArgumentException("unknown type: " + type);
      }
   }

   /**
    * Using the novel algorithm described here: http://www.homestarrunner.com/sbemail143.html .
    *
    * @return a random e-mail address
    */
   String randEmail() {

      // hobby (random letters)
      String email = "";
      int size = Math.abs(this.getRandom().nextInt() % 5) + 4;
      for (int i = 0; i < size; i++) {
         email += (char) (Math.abs(this.getRandom().nextInt() % 26) + 97);
      }

      // middle part
      switch (Math.abs(this.getRandom().nextInt() % 4)) {
         case 0:
            email += "kid";
            break;
         case 1:
            email += "grrl";
            break;
         case 2:
            email += "pie";
            break;
         default:
            email += "izzle";
            break;
      }

      // some numbers
      size = Math.abs(this.getRandom().nextInt() % 4) + 2;
      for (int i = 0; i < size; i++) {
         email += Math.abs(this.getRandom().nextInt() % 10);
      }

      // suffix, not "@kindergartencop.edu" for variety ;)
      email += "@";
      size = Math.abs(this.getRandom().nextInt() % 16) + 4;
      for (int i = 0; i < size; i++) {
         email += (char) (Math.abs(this.getRandom().nextInt() % 26) + 97);
      }
      return email + ".edu";
   }

   /**
    * Returns a new record ID where both the page ID and the slot number have the given value.
    *
    * @param val
    *           value for page ID and slot number
    * @return the record ID
    */
   private static RecordID newRecordID(final int val) {
      return new RecordID(PageID.getInstance(val), val);
   }
}

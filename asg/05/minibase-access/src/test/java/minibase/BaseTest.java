/*
 * @(#)BaseTest.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.ReplacementStrategy;
import minibase.storage.file.DiskManager;

/**
 * This base class contains common code to each layer's test suite.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public class BaseTest {

   /** Default database file name. */
   private static final String DB_PATH = System.getProperty("user.name") + ".minibase";

   /** Default database size (in pages). */
   protected static final int DB_SIZE = 20000;

   /** Default buffer pool size (in pages). */
   protected static final int BUF_SIZE = 17;

   /** Default buffer pool replacement policy. */
   private static final ReplacementStrategy BUF_POLICY = ReplacementStrategy.CLOCK;

   /** Initial seed for the random number generator. */
   private static final long INIT_SEED = 42;

   /** Random generator; use the same seed to make tests deterministic. */
   private final Random random = new Random(INIT_SEED);

   /**
    * Incremental history of the performance counters; odd elements are snapshots before the tests, and even
    * elements are after.
    */
   private List<CountData> counts;

   /** Reference to the database. */
   private Minibase minibase;

   /* -------------------------------------------------------------------------- */

   /**
    * Creates a new database on the disk.
    */
   @Before
   public void createMinibase() {
      System.out.println("Creating database...\n\tReplacement Policy: " + BaseTest.BUF_POLICY);
      this.minibase = new Minibase(BaseTest.DB_PATH, BaseTest.DB_SIZE, BaseTest.BUF_SIZE,
            BaseTest.BUF_POLICY, false);
   }

   /**
    * Deletes the database files from the disk.
    */
   @After
   public void deleteMinibase() {
      System.out.println("Deleting Minibase...");
      this.minibase.flush();
      this.minibase.getBufferManager().getDiskManager().destroy();
      this.minibase = null;
   }

   /* -------------------------------------------------------------------------- */

   /**
    * Resets the random generator to the default seed.
    */
   protected void initRandom() {
      // use the same seed every time in order to get reproducible tests
      this.random.setSeed(INIT_SEED);
   }

   /**
    * Gets the random number generator.
    *
    * @return random number generator
    */
   protected Random getRandom() {
      return this.random;
   }

   /**
    * Gets the disk manager.
    *
    * @return disk manager
    */
   protected DiskManager getDiskManager() {
      return this.minibase.getBufferManager().getDiskManager();
   }

   /**
    * Gets the buffer manager.
    *
    * @return buffer manager
    */
   protected BufferManager getBufferManager() {
      return this.minibase.getBufferManager();
   }

   /**
    * Resets the performance counter history.
    */
   protected void initCounts() {
      this.counts = new ArrayList<>();
   }

   /**
    * Saves the current performance counters, given the description.
    *
    * @param desc
    *           description of the measurements
    */
   protected void saveCounts(final String desc) {
      // create the new count data
      final CountData data = new CountData();
      this.counts.add(data);
      data.desc = desc;

      // save the counts (in correct order)
      this.minibase.flush();
      data.reads = this.minibase.getBufferManager().getDiskManager().getReadCount();
      data.writes = this.minibase.getBufferManager().getDiskManager().getWriteCount();
      data.allocs = this.minibase.getBufferManager().getDiskManager().getAllocCount();
      data.pinned = this.minibase.getBufferManager().getNumPinned();
   }

   /**
    * Prints the performance counters (i.e. for the current test).
    */
   protected void printCounters() {
      final CountData data = this.counts.get(this.counts.size() - 1);
      System.out.println();
      this.minibase.flush();
      System.out.println("  *** Number of reads:  "
            + (this.minibase.getBufferManager().getDiskManager().getReadCount() - data.reads));
      System.out.println("  *** Number of writes: "
            + (this.minibase.getBufferManager().getDiskManager().getWriteCount() - data.writes));
      System.out.println("  *** Net total pages:  "
            + (this.minibase.getBufferManager().getDiskManager().getAllocCount() - data.allocs));
      final int numbufs = this.minibase.getBufferManager().getNumBuffers();
      System.out.println("  *** Remaining Pinned: "
            + (numbufs - this.minibase.getBufferManager().getNumUnpinned()) + " / " + numbufs);
   }

   /**
    * Prints the complete history of the performance counters.
    *
    * @param sepcnt
    *           how many lines to print before each separator
    */
   protected void printSummary(final int sepcnt) {
      System.out.println();
      final String seperator = "--------------------------------------";
      System.out.println(seperator);
      System.out.println("\tReads\tWrites\tAllocs\tPinned");
      final int size = this.counts.size();
      for (int i = 1; i < size; i += 2) {

         if (i % (sepcnt * 2) == 1) {
            System.out.println(seperator);
         }

         final CountData before = this.counts.get(i - 1);
         final CountData after = this.counts.get(i);
         System.out.print(after.desc);

         System.out.print("\t" + (after.reads - before.reads));
         System.out.print("\t" + (after.writes - before.writes));
         System.out.print("\t" + (after.allocs - before.allocs));
         System.out.print("\t" + (after.pinned - before.pinned));
         System.out.println();
      }

      System.out.println(seperator);
   }

   /**
    * Counter values saved with a particular description.
    */
   class CountData {

      /** Description of the measurements. */
      private String desc;
      /** Number of read operations. */
      private int reads;
      /** Number of write operations. */
      private int writes;
      /** Number of allocated pages. */
      private int allocs;
      /** Number of pinned pages. */
      private int pinned;
   }
}

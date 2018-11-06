/*
 * @(#)BaseTest.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
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

   /** Allocation count and pin count when a test is started. */
   private int[] before;

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
    *
    * @throws Exception exception
    */
   @Before
   public final void createMinibase() throws Exception {
      this.minibase = this.createMinibaseInstance();
      final BufferManager bufferManager = this.getBufferManager();
      this.before = new int[] { bufferManager.getDiskManager().getAllocCount(), bufferManager.getNumPinned() };
   }

   /**
    * Closes the current minibase instance.
    *
    * @throws IOException I/O exception
    */
   protected void closeMinibase() throws IOException {
      this.minibase.close();
      this.minibase = null;
      this.before = null;
   }

   /**
    * Sets a new minibase instance.
    *
    * @param newMinibase new minibase
    */
   protected void setMinibase(final Minibase newMinibase) {
      if (this.minibase != null) {
         throw new IllegalStateException("unclosed minibase instance");
      }
      this.minibase = newMinibase;
      final BufferManager bufferManager = this.getBufferManager();
      this.before = new int[] { bufferManager.getDiskManager().getAllocCount(), bufferManager.getNumPinned() };
   }

   /**
    * Deletes the database files from the disk.
    *
    * @throws IOException if the database file cannot be deleted
    */
   @After
   public final void deleteMinibase() throws IOException {
      try {
         if (this.before != null) {
            final BufferManager bufferManager = this.getBufferManager();
            assertEquals("allocation count", this.before[0], bufferManager.getDiskManager().getAllocCount());
            assertEquals("pin count", this.before[1], bufferManager.getNumPinned());
            this.before = null;
         }
      } finally {
         this.minibase.delete();
         this.minibase = null;
      }
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
      return this.getMinibase().getBufferManager().getDiskManager();
   }

   /**
    * Gets the buffer manager.
    *
    * @return buffer manager
    */
   protected BufferManager getBufferManager() {
      return this.getMinibase().getBufferManager();
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
      this.getMinibase().flush();
      data.reads = this.getMinibase().getBufferManager().getDiskManager().getReadCount();
      data.writes = this.getMinibase().getBufferManager().getDiskManager().getWriteCount();
      data.allocs = this.getMinibase().getBufferManager().getDiskManager().getAllocCount();
      data.pinned = this.getMinibase().getBufferManager().getNumPinned();
   }

   /**
    * Prints the performance counters (i.e. for the current test).
    */
   protected void printCounters() {
      final CountData data = this.counts.get(this.counts.size() - 1);
      System.out.println();
      this.getMinibase().flush();
      System.out.println("  *** Number of reads:  "
            + (this.getMinibase().getBufferManager().getDiskManager().getReadCount() - data.reads));
      System.out.println("  *** Number of writes: "
            + (this.getMinibase().getBufferManager().getDiskManager().getWriteCount() - data.writes));
      System.out.println("  *** Net total pages:  "
            + (this.getMinibase().getBufferManager().getDiskManager().getAllocCount() - data.allocs));
      final int numbufs = this.getMinibase().getBufferManager().getNumBuffers();
      System.out.println("  *** Remaining Pinned: "
            + (numbufs - this.getMinibase().getBufferManager().getNumUnpinned()) + " / " + numbufs);
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
    * Current minibase instance.
    * 
    * @return minibase instance
    */
   protected final Minibase getMinibase() {
      return this.minibase;
   }

   /**
    * Create a new instance of minibase.
    * 
    * @return Minibase instance.
    * @throws Exception exception
    */
   protected Minibase createMinibaseInstance() throws Exception {
      return Minibase.createTemporary(this.getClass().getSimpleName(), BaseTest.DB_SIZE,
            BaseTest.BUF_SIZE, BaseTest.BUF_POLICY);
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

/*
 * @(#)HashIndexPerformanceBench.java   1.0   Jan 01, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import minibase.Minibase;
import minibase.RecordID;
import minibase.SearchKey;
import minibase.access.index.DataEntry;
import minibase.access.index.Index;
import minibase.access.index.IndexScan;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.ReplacementStrategy;

/**
 * Performance tests for the {@link LinearHashIndex} and {@link StaticHashIndexOld} using the jmh framework.
 * Before running the main method, be sure to build the sources with something like
 * {@code mvn clean install -DskipTests=true}.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
 * @version 1.0
 */
@Fork(1)
@Warmup(iterations = 5, time = 4)
@Measurement(iterations = 5, time = 30)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class HashIndexPerformanceBench {

   /**
    * Number of distinct keys to test.
    */
   @Param({ "1000", "10000" })
   private int numKeys;

   /**
    * Simple insert test of the {@link LinearHashIndex}.
    *
    * @param bench
    *           The bench configuration for the {@link LinearHashIndex}. It is provided by jmh.
    * @return the search key to prevent DCE as encouraged by jmh
    */
   // @Benchmark // Currently not in use
   @Threads(1)
   public SearchKey insertLin(final LinearIndexBench bench) {
      return insertTest(bench);
   }

   /**
    * Simple insert test of the {@link StaticHashIndexOld}.
    *
    * @param bench
    *           The bench configuration for the {@link StaticHashIndexOld}. It is provided by jmh.
    * @return the search key to prevent DCE as encouraged by jmh
    */
   // @Benchmark // Currently not in use
   @Threads(1)
   public SearchKey insertStat(final StaticIndexBench bench) {
      return insertTest(bench);
   }

   /**
    * Stress test of the {@link LinearHashIndex}.
    *
    * @param bench
    *           The bench configuration for the {@link LinearHashIndex}. It is provided by jmh.
    * @return the search key to prevent DCE as encouraged by jmh
    * @throws IOException
    *            if the underlying storage system has an exception
    */
   @Benchmark
   @Threads(1)
   public SearchKey stressLin(final LinearIndexBench bench) throws IOException {
      return this.stressTest(bench);
   }

   /**
    * Stress test of the {@link StaticHashIndexOld}.
    *
    * @param bench
    *           The bench configuration for the {@link StaticHashIndexOld}. It is provided by jmh.
    * @return the search key to prevent DCE as encouraged by jmh
    * @throws IOException
    *            if the underlying storage system has an exception
    */
   @Benchmark
   @Threads(1)
   public SearchKey stressStat(final StaticIndexBench bench) throws IOException {
      return this.stressTest(bench);
   }

   /**
    * Stress test the given index through one random insert, delete, search or scan operation.
    *
    * @param bench
    *           the {@link IndexBench} configuration for the test
    * @return the search key to prevent DCE
    * @throws IOException
    *            an I/O exception of the index
    */
   private SearchKey stressTest(final IndexBench bench) throws IOException {

      final int keyInt = bench.getRandom().nextInt(this.numKeys) - this.numKeys / 2;
      final SearchKey key = new SearchKey(keyInt);
      final int nextOp = bench.getRandom().nextInt(30001);
      if (nextOp < 10000) {
         // insert value
         final RecordID rid = newRecordID(Math.abs(keyInt));
         bench.getMap().computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
         bench.getIndex().insert(key, rid);
         assertEquals(rid, bench.getIndex().search(key).get().getRecordID());
      } else if (nextOp < 20000) {
         // random key lookup
         final Optional<DataEntry> data = bench.getIndex().search(key);
         RecordID rid = null;
         if (data.isPresent()) {
            rid = data.get().getRecordID();
            assertTrue(bench.getMap().getOrDefault(key, new ArrayList<>()).contains(rid));
         }
      } else if (nextOp < 30000) {
         // delete key
         final RecordID rid = newRecordID(Math.abs(keyInt));

         // delete key,rid pair from map
         final List<RecordID> records = bench.getMap().get(key);
         if (records != null) {
            records.remove(rid);
            if (records.size() == 0) {
               bench.getMap().remove(key);
            }
            // delete also from index
            bench.getIndex().remove(key, rid);
         }

         int newNum = 0;
         try (IndexScan scan = bench.getIndex().openScan(key)) {
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
         try (IndexScan fromKey = bench.getIndex().openScan(key)) {
            final Iterator<RecordID> mapIter = bench.getMap().getOrDefault(key, new ArrayList<>()).iterator();
            while (fromKey.hasNext()) {
               assertTrue(mapIter.hasNext());
               assertEquals(mapIter.next(), fromKey.next().getRecordID());
            }
            assertFalse(mapIter.hasNext());
         }
      }
      return key;
   }

   /**
    * Inserts a random search key into the given index.
    *
    * @param bench
    *           the {@link IndexBench} configuration
    * @return the search key to prevent DCE
    */
   private static SearchKey insertTest(final IndexBench bench) {
      final SearchKey key = new SearchKey(bench.randEmail());
      final RecordID rid = new RecordID(PageID.getInstance(0), 0);
      bench.getIndex().insert(key, rid);
      return key;
   }

   /**
    * Main method to run from Eclipse. Please do not forget to compile the project with maven before
    * executing: {@code mvn clean package -DskipTests=true}.
    *
    * @param args
    *           unused arguments
    * @throws RunnerException
    *            if the benchmark harness has problems running the code
    */
   public static void main(final String[] args) throws RunnerException {
      final org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
            .include(HashIndexPerformanceBench.class.getSimpleName())
            // report GC time
            .addProfiler(GCProfiler.class)
            // report method stack execution profile
            .addProfiler(StackProfiler.class).build();

      new Runner(opt).run();
   }

   /**
    * Benchmark configuration for the {@link LinearHashIndex}.
    *
    * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
    * @version 1.0
    */
   @State(Scope.Thread)
   public static class LinearIndexBench extends IndexBench {

      /**
       * Setup method for each set of benchmark iterations (trial).
       */
      @Setup(Level.Trial)
      public void setup() {
         System.out.println("Using LinearHashIndex.");
      }

      @Override
      @Setup(Level.Iteration)
      public void createIndex() {
         // TODO magic key size
         this.setIndex(LinearHashIndex.createTemporaryIndex(this.getMinibase().getBufferManager(), 56));
         /*
          * Note: We have to initialise the hash map here and not in the abstract class, because it seems that
          * jmh currently has problems running setup methods in abstract classes.
          */
         this.setMap(new HashMap<>());
      }
   }

   /**
    * Benchmark configuration for the {@link StaticHashIndexOld}.
    *
    * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
    * @version 1.0
    */
   @State(Scope.Thread)
   public static class StaticIndexBench extends IndexBench {

      /**
       * Setup method for each set of benchmark iterations (trial).
       */
      @Setup(Level.Trial)
      public void setup() {
         System.out.println("Using Static.");
      }

      @Override
      @Setup(Level.Iteration)
      public void createIndex() {
         // TODO magic key size
         this.setIndex(StaticHashIndex.createTemporaryIndex(this.getMinibase().getBufferManager(), 56));
         /*
          * Note: We have to initialise the hash map here and not in the abstract class, because it seems that
          * jmh currently has problems running setup methods in abstract classes.
          */
         this.setMap(new HashMap<>());
      }
   }

   /**
    * Common benchmark configuration for the index benchmarks.
    *
    * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt
    * @version 1.0
    */
   public abstract static class IndexBench {

      /**
       * A baseline map to hold what we are currently storing in the different indexes.
       */
      private Map<SearchKey, List<RecordID>> map;

      /**
       * The index under stress.
       */
      private Index index;

      /** Minibase instance. */
      private Minibase minibase;

      /** Initial seed for the random number generator. */
      private static final long INIT_SEED = 42;

      /** Random generator; use the same seed to make tests deterministic. */
      private final Random random = new Random(INIT_SEED);

      /**
       * Buffer pool sizes to test.
       */
      @Param({ "5", "10", "100" })
      private int numBuffers;

      /**
       * Resets the random generator to the default seed.
       */
      protected void initRandom() {
         // use the same seed every time in order to get reproducible tests
         this.random.setSeed(INIT_SEED);
      }

      /**
       * Creates the index. This is run before each iteration.
       */
      @Setup(Level.Iteration)
      public abstract void createIndex();

      /**
       * Deletes the index. This is run after each iteration.
       */
      @TearDown(Level.Iteration)
      public void deleteIndex() {
         this.getIndex().delete();
         this.setMap(new HashMap<>());
      }

      /**
       * Setup minibase before each trial.
       */
      @Setup(Level.Trial)
      public void setupMinibase() {
         this.minibase = new Minibase(HashIndexPerformanceBench.DB_PATH, HashIndexPerformanceBench.DB_SIZE,
               this.numBuffers, HashIndexPerformanceBench.BUF_POLICY, false);
         this.initRandom();
      }

      /**
       * Tear down minibase after each iteration.
       */
      @TearDown(Level.Trial)
      public void tearDownMinibase() {
         this.getMinibase().flush();
         this.getMinibase().getBufferManager().getDiskManager().destroy();
         this.minibase = null;
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
       * Using the novel algorithm described here: http://www.homestarrunner.com/sbemail143.html .
       *
       * @return a random e-mail address
       */
      private String randEmail() {

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
       * Gets the reference map.
       *
       * @return the map for reference
       */
      public Map<SearchKey, List<RecordID>> getMap() {
         return this.map;
      }

      /**
       * Sets the reference map.
       *
       * @param map
       *           the reference map
       */
      public void setMap(final Map<SearchKey, List<RecordID>> map) {
         this.map = map;
      }

      /**
       * Gets the index under test.
       *
       * @return the index under test
       */
      public Index getIndex() {
         return this.index;
      }

      /**
       * Sets the index under test.
       *
       * @param index
       *           the index under test
       */
      public void setIndex(final Index index) {
         this.index = index;
      }

      /**
       * Gets the minibase instance.
       *
       * @return the minibase instance
       */
      public Minibase getMinibase() {
         return this.minibase;
      }
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

   /** Default database file name. */
   private static final String DB_PATH = System.getProperty("user.name") + ".minibase";

   /** Default database size (in pages). */
   private static final int DB_SIZE = 200000;

   /** Default buffer pool replacement policy. */
   private static final ReplacementStrategy BUF_POLICY = ReplacementStrategy.CLOCK;
}

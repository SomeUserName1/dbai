/*
 * @(#)ExternalSortTest.java   1.0   Jan 10, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import minibase.BaseTest;
import minibase.access.file.FileScan;
import minibase.access.file.HeapFile;
import minibase.access.file.Run;
import minibase.access.file.RunBuilder;
import minibase.access.file.RunPage;
import minibase.access.file.RunScan;
import minibase.catalog.DataType;
import minibase.query.evaluator.compare.ByteArrayComparator;
import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.evaluator.compare.TupleComparator;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaBuilder;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.util.Convert;

/**
 * Test according to ex.1 & ex.5 .
 *
 * @author Fabian Klopfer, Simon Suckut
 */
// TODO rem permutations, bytewise comparator?
public class ExternalSortTest extends BaseTest {

   /** number of tuples to sort. */
   private int numTuplesToSort;
   /** the schema of the tuples to sort. */
   private Schema schema;
   /** seed for random number generation. */
   private long randomSeed;
   /**  the probability of duplicates in the generated test. */
   private double duplicatePropability;

   /** initialization of the random tuple generation. */
   @Before
   public void setUp() {
      this.numTuplesToSort = 1_000;
      this.schema = ExternalSortTest.sailorSchema();
      this.randomSeed = 1337;
      this.duplicatePropability = 0.01;
   }

   /**
    * Creates a temporary heap-file to test the insertion of different records.
    */
   @Test
   public void tempHeapFile() {
      try (HeapFile tempHeapFile = HeapFile.createTemporary(this.getBufferManager())) {
         this.createRandomSailorHeap(tempHeapFile, 160);

         // Print out all values via fileScanner
         final FileScan fileScan = tempHeapFile.openScan();
         while (fileScan.hasNext()) {
            final byte[] readData = fileScan.next();
            System.out.print(Convert.readInt(readData, 0) + "\t");
            System.out.print(Convert.readString(readData, 4, 50) + "  \t");
            System.out.print(Convert.readInt(readData, 54) + "\t");
            System.out.println(Convert.readFloat(readData, 58));
         }

         fileScan.close();
      }
   }

   /** use byte arrays and ascending sort order and run a test. */
   @Test
   public void testByteComparatorAscRandom() {
      final Operator inputOperatorRandom = new RandomTuples(this.schema, this.randomSeed,
            this.duplicatePropability, this.numTuplesToSort);
      final RecordComparator recordComparator = new ByteArrayComparator(this.schema.getLength());
      this.testOverallSortOrder(inputOperatorRandom, recordComparator);

      try (HeapFile tempHeapFile = HeapFile.createTemporary(this.getBufferManager())) {
         this.createRandomSailorHeap(tempHeapFile, this.numTuplesToSort);
         final Operator inputOperatorSorted = new HeapFileOperatorWrapper(this.schema, tempHeapFile);
         this.testOverallSortOrder(inputOperatorSorted, recordComparator);
      }
   }

    /**
     * Test with all possible permutations and orderings of tuples.
     */
   @Test
   public void testTupleComparatorAllFieldsCombinationsRandom() {
      final Operator inputOperatorRandom = new RandomTuples(this.schema, this.randomSeed,
            this.duplicatePropability, this.numTuplesToSort);

      // All field combinations of the schema to test
      final ArrayList<ArrayList<Integer>> allTestingOrders = new ArrayList<>();

      // Powerset of fields
      final ArrayList<ArrayList<Integer>> result = powerset(
            new int[] { 0, 1, 2, 3 }/* , 0, new ArrayList<Integer>(), result */);
      // For each set in powerset build all permutations
      for (ArrayList<Integer> set : result) {
         final ArrayList<ArrayList<Integer>> permutations = this.generatePerm(set, new ArrayList<>());
         allTestingOrders.addAll(permutations);
         System.out.println(set);
      }

      // For each permutation of field sets and each sort order permutation start the test once
      for (ArrayList<Integer> set : allTestingOrders) {
         // field permutation

         // Convert Integer ArrayList to int[]
         final int[] testingOrder = new int[set.size()];
         for (int i = 0; i < set.size(); i++) {
            testingOrder[i] = set.get(i);
         }

         final boolean[][] sortOrders = this.buildSortOrders(set.size());
         for (boolean[] sortOrder : sortOrders) {
            System.out.println("Field Permutation: " + set + "\t\t\t SortOrder Permutation: "
                    + Arrays.toString(sortOrder));

            final RecordComparator recordComparator = new TupleComparator(this.schema, testingOrder,
                    sortOrder);
            // Test with random input
            this.testOverallSortOrder(inputOperatorRandom, recordComparator);

            try (HeapFile tempHeapFile = HeapFile.createTemporary(this.getBufferManager())) {
               this.createRandomSailorHeap(tempHeapFile, this.numTuplesToSort);
               final Operator inputOperatorSorted = new HeapFileOperatorWrapper(this.schema, tempHeapFile);
               this.testOverallSortOrder(inputOperatorSorted, recordComparator);
            }
         }

         // Start test with this permutation
         this.testOverallSortOrder(inputOperatorRandom, new TupleComparator(this.schema, testingOrder));
      }
   }

   /**
    * Creates the random sailor file, sorts it and validates the output.
    *
    * @param inputOperator
    *           The operator delivering the input to sort
    * @param recordComparator
    *           The comparator to use including the sort order
    */
   private void testOverallSortOrder(final Operator inputOperator, final RecordComparator recordComparator) {
      // final int numTuplesToSort = 1_000;
      // final Schema schema = ExternalSortTest.sailorSchema();
      // final Operator inputOperator = new RandomTuples(schema, 1337, 0.01, numTuplesToSort);
      // final RecordComparator recordComparator = new ByteArrayComparator(schema.getLength());

      // Collection with all tuples in the random input
      final ArrayList<byte[]> allTuplesFromInput = new ArrayList<>(this.numTuplesToSort);
      final TupleIterator inputScan = inputOperator.open();
      while (inputScan.hasNext()) {
         allTuplesFromInput.add(inputScan.next());
      }

      // Prepare sort
      final ExternalSort sort = new ExternalSort(this.getBufferManager(), inputOperator, this.schema, recordComparator,
              10);

      // Build a run out of the sort to check it with checkRunIntegrity
      final RunBuilder runBuilder = new RunBuilder(this.getBufferManager(), this.schema.getLength());

      // Check if pass0-runs are sorted and no tuple is lost
      int totalTuples = 0;
      final TreeOfLosers tol = (TreeOfLosers) sort.open();
      final ArrayList<byte[]> allTuplesInTheCombinedRun = new ArrayList<>(this.numTuplesToSort);
      for (Run run : tol.getRuns()) {
         this.checkRunIntegrity(run, recordComparator, this.schema);
         totalTuples += run.getLength();

         final RunScan runScan = new RunScan(this.getBufferManager(), run.getFirstPageID(), run.getLength(),
               this.schema.getLength());
         while (runScan.hasNext()) {
            allTuplesInTheCombinedRun.add(runScan.next());
         }
         runScan.close();
      }
      tol.close();

      // Check if the concatenated pass 0 runs in sorted order equals the sorted input
      final ArrayList<byte[]> inputCopy1 = new ArrayList<>(allTuplesFromInput);
      while (0 < allTuplesInTheCombinedRun.size()) {
         // Do a manual indexOf Operation as byte[] is not proper compared by List.contains()

         final int indexOf = this.indexOfOnByteArray(allTuplesInTheCombinedRun.get(0), inputCopy1);

         // Remove both tuples from the list
         if (indexOf >= 0) {
            inputCopy1.remove(indexOf);
            allTuplesInTheCombinedRun.remove(0);
         } else {
            fail("Element of pass 0 run could not be found in original input list");
         }
      }
      // If one was not empty, not all tuples could be found again after pass 0
      assertEquals(0, inputCopy1.size());
      assertEquals(0, allTuplesInTheCombinedRun.size());

      // Number of tuples is equal to the number of tuples before pass 0
      assertEquals(this.numTuplesToSort, totalTuples);

      final TupleIterator sortScan = sort.open();

      byte[] currentMaximum = null;
      final ArrayList<byte[]> inputCopy2 = new ArrayList<>(allTuplesFromInput);
      while (sortScan.hasNext()) {
         final byte[] nextTuple = sortScan.next();
         System.out.println("Hello FROM HERE");
         System.out.print(Convert.readInt(nextTuple, 0) + "\t");
         System.out.print(Convert.readString(nextTuple, 4, 50) + "  \t");
         System.out.print(Convert.readInt(nextTuple, 54) + "\t");
         System.out.println(Convert.readFloat(nextTuple, 58));
         // Check if the returned tuple was already contained in the original input
         final int indexOf = this.indexOfOnByteArray(nextTuple, inputCopy2);
         if (indexOf < 0) {
            fail("next tuple could not be found in original input");
         }
         inputCopy2.remove(indexOf);

         // Check if the returned tuple is greater or equal than the one returned before.
         if (currentMaximum == null || recordComparator.greaterThanOrEqual(nextTuple, currentMaximum)) {
            currentMaximum = nextTuple;
         } else {
            fail(Arrays.toString(nextTuple) + " is smaller than " + Arrays.toString(currentMaximum));
         }

         runBuilder.appendRecord(nextTuple);
      }
      assertEquals(0, inputCopy2.size());

      final Run fullySortedRun = runBuilder.finish();
      assertEquals(this.numTuplesToSort, fullySortedRun.getLength());
      this.checkRunIntegrity(fullySortedRun, recordComparator, this.schema);

      // fullySortedRun.dealloc();
      Page<RunPage> currentPage = this.getBufferManager().pinPage(fullySortedRun.getFirstPageID());
      PageID nextPageID = RunPage.getNextPage(currentPage);
      this.getBufferManager().freePage(currentPage);
      while (!nextPageID.equals(PageID.INVALID)) {
         currentPage = this.getBufferManager().pinPage(nextPageID);
         nextPageID = RunPage.getNextPage(currentPage);
         this.getBufferManager().freePage(currentPage);
      }
      sortScan.close();
      try {
         inputScan.close();
      } catch (final UnsupportedOperationException ignored) {
      }
   }

   /** return the index of the current byte in the input table.
    *
    * @param searchTuple the tuple that we want the index of
    *
    * @param inputCopy1 the input table
    *
    * @return
    *   the index of the desired tuple
    */
   private int indexOfOnByteArray(final byte[] searchTuple, final ArrayList<byte[]> inputCopy1) {
      int indexOf = -1;
      for (int j = 0; j < inputCopy1.size(); j++) {
         if (Arrays.equals(inputCopy1.get(j), searchTuple)) {
            indexOf = j;
            break;
         }
      }
      return indexOf;
   }

   /**
    * Verifies the internal sort order inside of one run. Invokes assertion error if false.
    *
    * @param run
    *           the run to perform the integrity checks on
    * @param comparator
    *           Used to compare the order of the run
    * @param schema the schema of the input table
    */
   private void checkRunIntegrity(final Run run, final RecordComparator comparator, final Schema schema) {
      // Open scan and check that it is not empty
      final RunScan scan = new RunScan(this.getBufferManager(), run.getFirstPageID(), run.getLength(),
            schema.getLength());
      if (!scan.hasNext()) {
         scan.close();
         throw new IllegalArgumentException("Run to check was empty");
      }

      // Iterate over the scan and assert each time that the first record is smaller or equal than the second
      byte[] first = scan.next();
      byte[] second;
      while (scan.hasNext()) {
         second = scan.next();
         assertTrue(comparator.compare(first, second) <= 0);
         first = second;
      }

      scan.close();
   }

   /**
    * Fills the given Heapfile with random tuples of the sailor schema.
    * 
    * @param heapFile
    *           file to insert in
    * @param tuples
    *           How many pages shall the heap file span
    */
   private void createRandomSailorHeap(final HeapFile heapFile, final int tuples) {
      // Schema taken from the assignment's Exercise 1
      final Schema schemaStudents = ExternalSortTest.sailorSchema();

      // Fill with values until 10 pages must have been filled
      final int tupelSize = schemaStudents.getLength();
      for (int i = 0; i < tuples /* pageSize / tupelSize * tuples */; i++) {
         final byte[] insertionData = new byte[tupelSize];
         Convert.writeInt(insertionData, 0, i);
         Convert.writeString(insertionData, 4, "Sailor" + this.getRandom().nextInt(), 50);
         Convert.writeInt(insertionData, 54, this.getRandom().nextInt(11));
         Convert.writeFloat(insertionData, 58, this.getRandom().nextFloat() * 85.9f + 14f);
         heapFile.insertRecord(insertionData);
      }
   }

   /**
    * Returns the schema of the sailor relation.
    * 
    * @return The created schema
    */
   private static Schema sailorSchema() {
      return new SchemaBuilder()
            .addField("sid", DataType.INT, DataType.INT.getSize())
            .addField("sname", DataType.VARCHAR, 50)
            .addField("rating", DataType.INT, DataType.INT.getSize())
            .addField("age", DataType.FLOAT, DataType.FLOAT.getSize())
            .build();
   }

   /**
    * Source: https://www.geeksforgeeks.org/power-set/ .
    *
    * @param initialSet
    *           set used to build the power set from.
    * @return the power set of the initial set
    */
   private static ArrayList<ArrayList<Integer>> powerset(final int[] initialSet) {
      final int cardinalityPowerSet = (int) Math.pow(2, initialSet.length);
      final ArrayList<ArrayList<Integer>> powerset = new ArrayList<>(cardinalityPowerSet);
      for (int i = 0; i < cardinalityPowerSet; i++) {
         final ArrayList<Integer> set = new ArrayList<>(initialSet.length);
         for (int j = 0; j < initialSet.length; j++) {
            if ((i & (1 << j)) > 0) {
               set.add(initialSet[j]);
            }
         }
         powerset.add(set);
      }

      return powerset;
   }

   /**
    * Builds all permutations of initialSet.
    *
    * @param initialSet
    *           set to build permutations on.
    * @param usedOrder
    *           initialize this with null. Used to remember the already taken elements in the recursion tree.
    * @return
    *       all permutations of the initial set
    */
   @SuppressWarnings("unchecked")
   private ArrayList<ArrayList<Integer>> generatePerm(final ArrayList<Integer> initialSet,
                                                      final ArrayList<Integer> usedOrder) {
      final ArrayList<ArrayList<Integer>> returnVal = new ArrayList<>();
      if (usedOrder.size() == initialSet.size()) {
         returnVal.add(usedOrder);
         return returnVal;
      }

      for (int i = 0; i < initialSet.size(); i++) {
         final ArrayList<Integer> initialSetCopy = (ArrayList<Integer>) initialSet.clone();
         final ArrayList<Integer> usedOrderCopy = (ArrayList<Integer>) usedOrder.clone();
         usedOrderCopy.add(initialSet.get(i));
         initialSetCopy.remove(i);
         returnVal.addAll(this.generatePerm(initialSetCopy, usedOrderCopy));
      }

      return returnVal;
   }

   /**
    * Builds sort orders.
    *
    * @param n
    *           How many fields to sort. This will be the size of the output
    * @return Sort orders of lenghth n
    */
   private boolean[][] buildSortOrders(final int n) {
      if (n > 1) {
         final boolean[][] recursed = this.buildSortOrders(n - 1);
         final boolean[][] newArray = new boolean[recursed.length * 2][];
         System.arraycopy(recursed, 0, newArray, 0, recursed.length);
         System.arraycopy(recursed, 0, newArray, recursed.length, recursed.length);

         for (int i = 0; i < newArray.length; i++) {
            final boolean[] newSet = new boolean[newArray[i].length + 1];
            System.arraycopy(newArray[i], 0, newSet, 1, newArray[i].length);
             newSet[0] = i < newArray.length / 2;
            newArray[i] = newSet;
         }

         return newArray;
      } else {
         return new boolean[][] { new boolean[] { false }, new boolean[] { true } };
      }
   }

   /**
    * Used to make an operator out of a heap file.
    *
    * @author Fabian Klopfer, Simon Suckut
    */
   public class HeapFileOperatorWrapper implements Operator {

      /** Schema of the HeapFile. */
      private final Schema schema;

      /** HeapFile to later build a TupleIterator from. */
      private final HeapFile heapFile;

      /** Wrapper class for a heap file and the schema contained in the heap file.
       * @param heapFile the heap file
       * @param schema the schema
       */
      HeapFileOperatorWrapper(final Schema schema, final HeapFile heapFile) {
         this.schema = schema;
         this.heapFile = heapFile;
      }

      @Override
      public TupleIterator open() {
         return new FileScanTupleIteratorWrapper(this.heapFile);
      }

      @Override
      public Schema getSchema() {
         return this.schema;
      }
   }

   /**
    * Used to make an tuple iterator out of a FileScan.
    *
    * @author Fabian Klopfer, Simon Suckut
    */
   public class FileScanTupleIteratorWrapper implements TupleIterator {

      /** Filescan to redirct TupleIterator Methods to. */
      private final FileScan fileScan;

      /**
       * Instantiates a TupleIterator which uses the FileIterator of the given file to direct it's operations.
       *
       * @param heapFile
       *           File to iterate over.
       */
      FileScanTupleIteratorWrapper(final HeapFile heapFile) {
         this.fileScan = heapFile.openScan();
      }

      @Override
      public boolean hasNext() {
         return this.fileScan.hasNext();
      }

      @Override
      public byte[] next() {
         return this.fileScan.next();
      }

      @Override
      public void reset() {
         this.fileScan.reset();
      }

      @Override
      public void close() {
         this.fileScan.close();
      }
   }
}

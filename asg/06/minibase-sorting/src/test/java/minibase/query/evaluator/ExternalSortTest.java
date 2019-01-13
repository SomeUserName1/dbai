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
public class ExternalSortTest extends BaseTest {

    /**
     * number of tuples to sort.
     */
    private int numTuplesToSort;
    /**
     * the schema of the tuples to sort.
     */
    private Schema schema;
    /**
     * seed for random number generation.
     */
    private long randomSeed;
    /**
     * the probability of duplicates in the generated test.
     */
    private double duplicatePropability;

    /**
     * initialization of the random tuple generation.
     */
    @Before
    public void setUp() {
        this.numTuplesToSort = 9;
        this.schema = ExternalSortTest.sailorSchema();
        this.randomSeed = 42;
        this.duplicatePropability = 0.01;
    }

    /**
     * Creates a temporary heap-file to test the insertion of different records.
     */
    @Test
    public void tempHeapFile() {
        try (HeapFile tempHeapFile = HeapFile.createTemporary(this.getBufferManager())) {
            System.out.println("_________Test tempHeapFile_____________");
            this.createRandomSailorHeap(tempHeapFile, 9, true);

            // Print out all values via fileScanner
            final FileScan fileScan = tempHeapFile.openScan();
            this.printSailor(fileScan);

            fileScan.close();
        }
    }

    /**
     * fukuruk.
     */
    @Test
    public void simplePrint() {
        System.out.println("_________Test simplePrint_____________");
        final RecordComparator recordComparator = new ByteArrayComparator(this.schema.getLength());

        final Operator inputOperatorRandom = new RandomTuples(this.schema, this.randomSeed,
                this.duplicatePropability, this.numTuplesToSort);
        final ExternalSort sortRand = new ExternalSort(this.getBufferManager(), inputOperatorRandom, this.schema,
                recordComparator, 10);
        final TupleIterator rIt = inputOperatorRandom.open();
        System.out.println("__Random Input___");
        while (rIt.hasNext()) {
            System.out.println(Arrays.toString(rIt.next()));
        }
        final TupleIterator tir = sortRand.open();
        System.out.println("__After Sorting__");
        while (tir.hasNext()) {
            System.out.println(Arrays.toString(tir.next()));
        }
        tir.close();

        try (HeapFile tempHeapFile = HeapFile.createTemporary(this.getBufferManager())) {
            this.createRandomSailorHeap(tempHeapFile, this.numTuplesToSort, false);
            final HeapFileOperator inputOperatorSorted = new HeapFileOperator(this.schema, tempHeapFile);
            final TupleIterator it = inputOperatorSorted.open();
            System.out.println("__HeapFile___");
            this.printSailor(it);
            it.close();

            final ExternalSort sort = new ExternalSort(this.getBufferManager(), inputOperatorSorted, this.schema,
                    recordComparator, 10);
            final TupleIterator ti = sort.open();
            System.out.println("__After Sorting__");
            this.printSailor(ti);
            ti.close();
        }
    }

    /**
     * Some stupid print function.
     *
     * @param ti tuple iterator to print
     */
    private void printSailor(final TupleIterator ti) {
        while (ti.hasNext()) {
            final byte[] nextTuple = ti.next();
            System.out.print(Convert.readInt(nextTuple, 0) + "\t");
            System.out.print(Convert.readString(nextTuple, 4, 50) + "  \t");
            System.out.print(Convert.readInt(nextTuple, 54) + "\t");
            System.out.println(Convert.readFloat(nextTuple, 58));
        }
    }

    /**
     * use byte arrays and ascending sort order and run a test.
     */
    @Test
    public void testAsc() {
        final Operator inputOperatorRandom = new RandomTuples(this.schema, this.randomSeed,
                this.duplicatePropability, this.numTuplesToSort);
        final RecordComparator recordComparator = new ByteArrayComparator(this.schema.getLength());
        this.testOrder(inputOperatorRandom, recordComparator);

        try (HeapFile tempHeapFile = HeapFile.createTemporary(this.getBufferManager())) {
            this.createRandomSailorHeap(tempHeapFile, this.numTuplesToSort, true);
            final Operator inputOperatorSorted = new HeapFileOperator(this.schema, tempHeapFile);
            this.testOrder(inputOperatorSorted, recordComparator);
        }
    }

    /**
     * Creates the random sailor file, sorts it and validates the output.
     *
     * @param inputOperator    The operator delivering the input to sort
     * @param recordComparator The comparator to use including the sort order
     */
    private void testOrder(final Operator inputOperator, final RecordComparator recordComparator) {

        final ExternalSort checkRun = new ExternalSort(this.getBufferManager(), inputOperator, this.schema,
                recordComparator, 10);

        // Check overall sort order
        final TupleIterator sortScan = checkRun.open();
        final RunBuilder runBuilder = new RunBuilder(this.getBufferManager(), this.schema.getLength());
        int totalTuples = 0;
        while (sortScan.hasNext()) {
            runBuilder.appendRecord(sortScan.next());
            ++totalTuples;
        }
        final Run r = runBuilder.finish();
        this.validate(r, recordComparator, this.schema);
        assertEquals(this.numTuplesToSort, totalTuples);

        Page<RunPage> currentPage = this.getBufferManager().pinPage(r.getFirstPageID());
        PageID nextPageID = RunPage.getNextPage(currentPage);
        this.getBufferManager().freePage(currentPage);
        while (!nextPageID.equals(PageID.INVALID)) {
            currentPage = this.getBufferManager().pinPage(nextPageID);
            nextPageID = RunPage.getNextPage(currentPage);
            this.getBufferManager().freePage(currentPage);
        }

        // Check pass0
        totalTuples = 0;
        final TreeOfLosers tol = (TreeOfLosers) sortScan;
        for (Run run : tol.getRuns()) {
            this.validate(run, recordComparator, this.schema);
            totalTuples += run.getLength();
        }
        assertEquals(this.numTuplesToSort, totalTuples);

        tol.close();
    }

    /**
     * Verifies the sort order.
     *
     * @param run        the run
     * @param comparator comparator
     * @param schema     the schema
     */
    private void validate(final Run run, final RecordComparator comparator, final Schema schema) {
        final RunScan scan = new RunScan(this.getBufferManager(), run.getFirstPageID(), run.getLength(),
                schema.getLength());
        if (!scan.hasNext()) {
            scan.close();
            fail("Run to check was empty");
        }

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
     * see ex. 1.
     *
     * @param heapFile heap file to insert to
     * @param tuples   number of tuples to insert
     * @param asc      flag weather to index ascending or descending
     */
    private void createRandomSailorHeap(final HeapFile heapFile, final int tuples, final boolean asc) {

        final int tupelSize = ExternalSortTest.sailorSchema().getLength();
        int index;
        for (int i = 0; i < tuples; ++i) {
            final byte[] insertionData = new byte[tupelSize];
            index = asc ? i : tuples - 1 - i;
            Convert.writeInt(insertionData, 0, index);
            Convert.writeString(insertionData, 4, "Sailor" + this.getRandom().nextInt(), 50);
            Convert.writeInt(insertionData, 54, this.getRandom().nextInt(11));
            Convert.writeFloat(insertionData, 58, this.getRandom().nextFloat() * 85.9f + 14f);
            heapFile.insertRecord(insertionData);
        }
    }

    /**
     * See ex.1.
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
     * Used to make an operator out of a heap file.
     */
    public class HeapFileOperator implements Operator {

        /**
         * Schema of the HeapFile.
         */
        private final Schema schema;

        /**
         * HeapFile to later build a TupleIterator from.
         */
        private final HeapFile heapFile;


        /**
         * Wrapper class for a heap file and the schema contained in the heap file.
         *
         * @param heapFile the heap file
         * @param schema   the schema
         */
        HeapFileOperator(final Schema schema, final HeapFile heapFile) {
            this.schema = schema;
            this.heapFile = heapFile;
        }

        @Override
        public TupleIterator open() {
            return this.heapFile.openScan();
        }

        @Override
        public Schema getSchema() {
            return this.schema;
        }
    }
}

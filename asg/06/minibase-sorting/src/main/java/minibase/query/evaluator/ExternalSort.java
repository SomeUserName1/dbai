/*
 * @(#)ExternalSort.java   1.0   Jan 10, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */

package minibase.query.evaluator;

import java.util.ArrayList;

import minibase.access.file.Run;
import minibase.access.file.RunBuilder;
import minibase.access.file.RunPage;
import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.schema.Schema;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.file.DiskManager;

/**
 * Implements Ex 3 with ReplacementSort.
 *
 * @author Fabian Klopfer, Simon Suckut
 */
public class ExternalSort extends AbstractOperator implements TupleIterator {

    /**
     * The buffer manager.
     */
    private final BufferManager bufferManager;

    /**
     * A TupleIterator over the input gotten from an operator.
     */
    private final TupleIterator inputIterator;

    /**
     * The schema of the input.
     */
    private final Schema schema;

    /**
     * The comparator which shall be sorted according to.
     */
    private final RecordComparator comparator;

    /**
     * How many pages is the replacement sort allowed to use.
     */
    private final int bufferSize;

    /**
     * Pages for the buffer to use.
     */
    private final ArrayList<Page<RunPage>> pages;

    /**
     * How many records fit on a page.
     */
    private final int recordsPerPage;

    /**
     * How many records are currently in the buffer.
     */
    private int recordsInBuffer;

    /**
     * The greatest by add returned record.
     */
    private byte[] greatestRecord;

    /**
     * If this buffer is invalidated (no more usable).
     */
    private boolean invalidated;

    /**
     * Saves record from input if a new run had to be initialized.
     */
    private byte[] danglingRecord;

    /**
     * Constructor.
     *
     * @param schema        Schema of the internal tuples
     * @param bufferManager where the pages come from
     * @param input         operator giving the input data for the sort
     * @param comparator    Compares the tuples with each other
     * @param bufferSize    How many pages can be used for the currentSet in Replacement Sort.
     */
    ExternalSort(final BufferManager bufferManager, final Operator input, final Schema schema,
                 final RecordComparator comparator, final int bufferSize) {
        super(schema);

        this.bufferManager = bufferManager;
        this.inputIterator = input.open();
        this.schema = schema;
        this.comparator = comparator;
        this.bufferSize = bufferSize;
        this.recordsPerPage = (DiskManager.PAGE_SIZE - 4) / this.schema.getLength();

        this.pages = new ArrayList<>(this.bufferSize);
        this.greatestRecord = null;
        this.danglingRecord = null;


        final int maxRecords = this.getMaxRecords();

        for (this.recordsInBuffer = 0; this.recordsInBuffer < maxRecords && this.inputIterator.hasNext();
             ++this.recordsInBuffer) {

            if (this.recordsInBuffer % this.recordsPerPage == 0) {
                // If there is no page allocated (initial iteration) or the page is full, allocate a new one
                this.pages.add(RunPage.newPage(this.bufferManager, PageID.INVALID));
            }
            RunPage.insertEntry(this.pages.get(this.pages.size() - 1), this.inputIterator.next(), this.recordsInBuffer,
                    this.schema.getLength(), this.recordsPerPage);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator open() {
        final ArrayList<Run> runs = new ArrayList<>();
        RunBuilder runBuilder = new RunBuilder(this.bufferManager, this.schema.getLength());

        while (this.hasNext()) {
            final byte[] replacement = this.next();

            if (replacement != null) {
                runBuilder.appendRecord(replacement);
            } else {
                runs.add(runBuilder.finish());
                runBuilder = new RunBuilder(this.bufferManager, this.schema.getLength());
                // not resetting the sort but only the TupleIterator in order to fill the new Run
                this.reset();
            }
        }
        runs.add(runBuilder.finish());

        this.close();

        try {
            this.inputIterator.close();
        } catch (final UnsupportedOperationException ignored) {
        }
        // TODO merge and return final Run
        //RunBuilder result = new RunBuilder(this.bufferManager, this.schema.getLength());
        //TreeOfLosers tol = new TreeOfLosers(runs, this.comparator, this.schema.getLength(), this.bufferManager);


        return new TreeOfLosers(runs, this.comparator, this.schema.getLength(), this.bufferManager);
    }

    /**
     * Adds the given record to the buffer. If buffer is full, the least record greater than greatestRecord
     * will be replaced returned.
     *
     * @return the next record
     */
    public byte[] next() {
        final byte[] record;
        if (this.danglingRecord != null) {
            record = this.danglingRecord;
            this.danglingRecord = null;
        } else {
            record = this.inputIterator.hasNext() ? this.inputIterator.next() : null;
        }
        this.isInvalidated();

        byte[] least = null;
        int position = -1;

        // Find the least of the remaining records w.r.t. comparator that is greater than the current greatest
        for (int i = 0; i < this.recordsInBuffer; ++i) {

            final int slot = i % recordsPerPage;
            final Page<RunPage> page = this.pages.get(i / this.recordsPerPage);
            final byte[] candidate = RunPage.getTuple(page, slot, this.schema.getLength());

            if ((least == null || this.comparator.lessThan(candidate, least)) && (this.greatestRecord == null
                    || this.comparator.greaterThanOrEqual(candidate, this.greatestRecord))) {
                least = candidate;
                position = i;
            }
        }
        // Check if the next input record is better fitting
        if (record != null && (least == null || this.comparator.lessThan(record, least))
                && (this.greatestRecord == null || this.comparator.greaterThanOrEqual(record, this.greatestRecord))) {
                this.greatestRecord = record;
                return record;
        } else if (least == null) {
            // No satisfying element could be found
            this.danglingRecord = record;
            return null;
        }

        // fill leasts buffer space with the next input record
        if (record != null) {
            final Page<RunPage> replacementPage = this.pages
                    .get(position / this.recordsPerPage);
            RunPage.setTuple(replacementPage, position % this.recordsPerPage, record,
                    this.schema.getLength());
        } else {
            // Replacment not possible, move records.
            this.recordsInBuffer--;
            for (int i = position; i < this.recordsInBuffer; ++i) {
                RunPage.setTuple(
                        this.pages.get(i / this.recordsPerPage), i % this.recordsPerPage,
                        RunPage.getTuple(
                                this.pages.get((i + 1) / this.recordsPerPage), (i + 1) % this.recordsPerPage,
                                this.schema.getLength()
                        ),
                        this.schema.getLength()
                );
            }
        }
        // set least as the new greatest record in the run and return it to be appended
        this.greatestRecord = least;
        return least;
    }

    /**
     * Returns true if this scan was not closed before. Otherwise false.
     */
    private void isInvalidated() {
        if (this.invalidated) {
            throw new IllegalStateException("This run scan was already closed");
        }
    }

    @Override
    public boolean hasNext() {
        return this.recordsInBuffer > 0;
    }

    /**
     * resets the TupleIterator (only depends on the greatest tuple) to add a new Run.
     */
    @Override
    public void reset() {
        this.greatestRecord = null;
    }

    @Override
    public void close() {
        this.isInvalidated();
        this.invalidated = true;
        for (Page<RunPage> runPage : this.pages) {
            this.bufferManager.freePage(runPage);
        }
    }

    /**
     *
     * @return
     *  the maxmimal number of records in the buffer
     */
    private int getMaxRecords() {
        return this.bufferSize * this.recordsPerPage;
    }
}

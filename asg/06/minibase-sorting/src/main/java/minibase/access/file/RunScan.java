
package minibase.access.file;

import minibase.query.evaluator.TupleIterator;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 *  Iterator to return values of a run.
 *
 * @author Fabian Klopfer, Simon Suckut
 */
public class RunScan implements TupleIterator {

    /**
     * Buffer Manager to pin the pages.
     */
    private final BufferManager bufferManager;

    /**
     * ID of the first page of the run.
     */
    private final PageID firstPageID;

    /**
     * Current page.
     */
    private Page<RunPage> currentPage;

    /**
     * Number of records that have already been read.
     */
    private int numRecordsRead;

    /**
     * Total number of Records in the run.
     */
    private long totalNumRecords;

    /**
     * Length of each record in the pages.
     */
    private final int recordLength;

    /**
     * How many records have space on each page.
     */
    private final int recordsPerPage;

    /**
     * If true this runscan can't be used anymore.
     */
    private boolean invalidated;

    /**
     * Creating the run scan with all necessary parameters.
     *
     * @param bufferManager   Where the pages come from
     * @param firstPageID     ID of the first page of the run
     * @param totalNumRecords How many records are in the run
     * @param recordLength    How long is each record
     */
    public RunScan(final BufferManager bufferManager, final PageID firstPageID, final long totalNumRecords,
                   final int recordLength) {
        this.bufferManager = bufferManager;
        this.firstPageID = firstPageID;
        this.currentPage = this.bufferManager.pinPage(firstPageID);
        this.totalNumRecords = totalNumRecords;
        this.recordLength = recordLength;
        this.recordsPerPage = RunPage.maxNumberOfRecords(this.recordLength);
        this.invalidated = false;

        this.reset();
    }

    @Override
    public boolean hasNext() {
        this.invalidated();
        return this.numRecordsRead < this.totalNumRecords;
    }

    @Override
    public byte[] next() {
        this.invalidated();
        if (this.numRecordsRead % this.recordsPerPage == 0 && this.numRecordsRead != 0) {
            // Page completely read. Get next page.
            final PageID nextPageID = RunPage.getNextPage(this.currentPage);
            this.bufferManager.unpinPage(this.currentPage, UnpinMode.CLEAN);
            this.currentPage = this.bufferManager.pinPage(nextPageID);
        }

        return RunPage.getTuple(this.currentPage, this.numRecordsRead++ % this.recordsPerPage,
                this.recordLength);
    }

    @Override
    public void reset() {
        this.invalidated();
        this.bufferManager.unpinPage(this.currentPage, UnpinMode.CLEAN);
        this.currentPage = this.bufferManager.pinPage(this.firstPageID);
        this.numRecordsRead = 0;
    }

    @Override
    public void close() {
        this.invalidated();
        this.bufferManager.unpinPage(this.currentPage, UnpinMode.CLEAN);
        this.invalidated = true;
    }

    /**
     * Returns false if this scan has been invalidated, true otherwise.
     */
    private void invalidated() {
        if (this.invalidated) {
            throw new IllegalStateException("This run scan was already closed");
        }
    }
}

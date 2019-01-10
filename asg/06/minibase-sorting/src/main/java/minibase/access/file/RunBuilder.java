/*
 * @(#)RunBuilder.java   1.0   Dec 29, 2018
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.file;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;
import minibase.storage.file.DiskManager;

/**
 * Adds tuples repeatingly to a RunPage and creates a Run when finished.
 * *
 *
 * @author Fabian Klopfer, Simon Suckut
 */
public final class RunBuilder {

    /**
     * Buffer Manager to allocate and contain pages.
     */
    private final BufferManager bufferManager;

    /**
     * Length of a record in bytes.
     */
    private final int recordLength;

    /**
     * Maximal amount of records per page.
     */
    private final int maxRecordsPerPage;

    /**
     * Overall number of records.
     */
    private long numRecords;

    /**
     * Current page.
     */
    private Page<RunPage> current;

    /**
     * ID of the first page.
     */
    private final PageID firstPageID;

    /**
     * Constructs the builder.
     *
     * @param bufferManager the BufferManager to allocate and pin/unpin pages with
     * @param recordLength  Length of a record to be contained in the run
     */
    public RunBuilder(final BufferManager bufferManager, final int recordLength) {
        this.bufferManager = bufferManager;
        this.recordLength = recordLength;
        this.maxRecordsPerPage = (DiskManager.PAGE_SIZE - 4) / recordLength;
        this.numRecords = 0;

        this.current = RunPage.newPage(this.bufferManager, PageID.INVALID);
        this.firstPageID = this.current.getPageID();
    }

    /**
     * Appends a record to the to be built run.
     *
     * @param record to insert
     */
    public void appendRecord(final byte[] record) {
        if ((this.numRecords % maxRecordsPerPage) == 0 && this.numRecords != 0) {
            // The current page is full. Add a new page, adjusting pointers,
            // unpin the previous page and set the new page to be the current one
            final Page<RunPage> newPage = RunPage.newPage(this.bufferManager, this.current.getPageID());
            this.bufferManager.unpinPage(this.current, UnpinMode.DIRTY);
            this.current = newPage;
        }

        RunPage.setTuple(this.current, (int) (this.numRecords++ % this.maxRecordsPerPage), record, this.recordLength);
    }

    /**
     * Builds a run of the created RunPages.
     *
     * @return the run
     */
    public Run finish() {
        this.bufferManager.unpinPage(this.current, UnpinMode.DIRTY);
        return new Run(this.firstPageID, this.numRecords);
    }
}

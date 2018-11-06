/*
 * @(#)BufferManager.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

import minibase.storage.file.DiskManager;

/**
 * The buffer manager manages the buffer pool, which consists of a collection of slots (so-called buffer pages) that
 * can hold pages, which have been read into memory. Internally, this buffer pool is represented as an array
 * of page objects. The buffer manager reads disk pages into a main memory page as needed and evicts pages
 * from memory that are no longer required, if the buffer pool has no empty slots to hold new pages. The
 * buffer manager provides the following services.
 * <ul>
 * <li>Pinning and unpinning disk pages to and from buffer.</li>
 * <li>Allocating and deallocating sequences of disk pages and coordinating this with the buffer pool.</li>
 * <li>Reading data from disk into memory pages.</li>
 * <li>Flushing pages from the buffer pool to disk.</li>
 * </ul>
 * The buffer manager is used by access methods, heap files, and relational operators to read, write,
 * allocate, and deallocate pages.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public interface BufferManager {

    /**
     * Returns the disk manager used by this buffer manager.
     *
     * @return the underlying disk manager
     */
    DiskManager getDiskManager();

    /**
     * Allocates a new page, pins it and fills the page contents with zeroes.
     *
     * @return the pinned page
     * @throws IllegalStateException
     *            if all pages are pinned (i.e. pool exceeded)
     */
    Page<?> newPage();

    /**
     * Deallocates a single page from disk, freeing it from the pool if needed. The page must be pinned
     * exactly once when this method is called.
     *
     * @param page
     *           the page to free
     * @throws IllegalArgumentException
     *            if the page is pinned more or less than one time
     */
    void freePage(Page<?> page);

    /**
     * Pins a disk page into the buffer pool. If the page is already pinned, this simply increments the pin
     * count. Otherwise, this method selects an empty slot and reads the page from disk into the buffer
     * pool. If there are no empty slots, the replacement policy of the buffer manager is used to select a
     * victim page. The victim page is then evicted from the buffer pool, flushing it to disk if it is dirty
     * and the new page is loaded into its slot. Finally, the buffer containing the new page is returned.
     *
     * @param pageID
     *           identifies the page to pin
     * @return buffer holding the page contents
     * @param <T>
     *           type of the pinned page (not checked)
     * @throws IllegalStateException
     *           if the buffer pool is full and all pages are pinned
     */
    <T extends PageType> Page<T> pinPage(PageID pageID);

    /**
     * Unpins a disk page from the buffer pool, decreasing its pin count.
     *
     * @param page
     *           page to unpin
     * @param mode
     *           {@link UnpinMode#DIRTY} if the page was modified, {@link UnpinMode#CLEAN} otherwise
     * @throws IllegalArgumentException
     *            if the page is not present in the buffer pool or not pinned
     */
    void unpinPage(Page<?> page, UnpinMode mode);

    /**
     * Immediately writes a page in the buffer pool to disk, if dirty. Note that flushing a page only writes
     * that page to disk, i.e., the page will not be unpinned, freed, etc.
     *
     * @param page
     *           page to be flushed
     */
    void flushPage(Page<?> page);

    /**
     * Immediately writes all dirty pages in the buffer pool to disk. Note that flushing a page only writes
     * that page to disk, i.e., the page will not be unpinned, freed, etc.
     */
    void flushAllPages();

    /**
     * Returns the total number of buffer pages.
     *
     * @return total number of buffer pages
     */
    int getNumBuffers();

    /**
     * Returns the total number of pinned buffer pages.
     *
     * @return total number of pinned buffer pages
     */
    int getNumPinned();

    /**
     * Returns the total number of unpinned buffer pages.
     *
     * @return total number of unpinned buffer pages
     */
    int getNumUnpinned();
}

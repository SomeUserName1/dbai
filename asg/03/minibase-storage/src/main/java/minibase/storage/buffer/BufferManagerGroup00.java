/*
 * @(#)BufferManagerGroup00.java   1.0   Nov 06, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

import minibase.storage.buffer.policy.ReplacementPolicy;
import minibase.storage.file.DiskManager;

import java.util.Arrays;
import java.util.HashMap;

public final class BufferManagerGroup00 implements BufferManager {

    private final DiskManager diskManager;
    private final int numBuffers;
    private final ReplacementPolicy replacementPolicy;
    private Page<?>[] bufferPages;
    private HashMap<PageID, Integer> pageToSlot;



    public BufferManagerGroup00(final DiskManager diskManager, final int bufferPoolSize,
                                final ReplacementStrategy replacementStrategy) {
        this.diskManager = diskManager;
        this.numBuffers = bufferPoolSize;
        this.replacementPolicy = replacementStrategy.newInstance(this.numBuffers);
        this.bufferPages = new Page<?>[bufferPoolSize];
        this.pageToSlot = new HashMap<>();

    }

    @Override
    public DiskManager getDiskManager() { return this.diskManager; }

    @Override
    public Page<?> newPage() { // TODO
        PageID nPageID = this.diskManager.allocatePage();
        Page nPage = this.pinPage(nPageID);
        Arrays.fill(nPage.getData(), (byte) 0);
        nPage.setDirty(true);
        flushPage(nPage);
        return nPage;
    }

    @Override
    public void freePage(final Page<?> page) { // TODO

    }

    @Override
    public <T extends PageType> Page<T> pinPage(final PageID pageID) {
        if(pageToSlot.containsKey(pageID)) {
            Page<T> page = (Page<T>)bufferPages[pageToSlot.get(pageID)];
            page.incrementPinCount();
            return  page;
        } else if (this.getNumUnpinned() > 1) {
            int victim_slot = replacementPolicy.pickVictim();
            Page victim_page = bufferPages[victim_slot];
            flushPage(victim_page);
            victim_page.reset(pageID);
            diskManager.readPage(pageID, victim_page.getData());
            replacementPolicy.stateChanged(victim_slot, ReplacementPolicy.PageState.PINNED);
            pageToSlot.put(pageID, victim_slot);
            victim_page.incrementPinCount();
            return (Page<T>)victim_page;
        }
        throw new IllegalStateException("No free pages available");
    }

    @Override
    public void unpinPage(final Page<?> page, final UnpinMode mode) {
        if (!pageToSlot.containsKey(page.getPageID())) throw new IllegalStateException("Page" + page.getPageID() +
                "not in Memory");

        if (mode == UnpinMode.CLEAN) page.setDirty(false); else page.setDirty(true);

        page.decrementPinCount();
        if( page.getPinCount() < 1)  {
            PageID pageID = page.getPageID();
            replacementPolicy.stateChanged(pageToSlot.get(pageID), ReplacementPolicy.PageState.UNPINNED);
            pageToSlot.remove(pageID);
        }
    }

    @Override
    public void flushPage(final Page<?> page) {
        if (page.isDirty()) {
            // write the page to disk
            this.diskManager.writePage(page.getPageID(), page.getData());
            // the buffer page is now clean
            page.setDirty(false);
        }
    }

    @Override
    public void flushAllPages() { for( Page page : this.bufferPages) flushPage(page); }

    @Override
    public int getNumBuffers() { return this.numBuffers; }

    @Override
    public int getNumPinned() { return pageToSlot.size(); }

    @Override
    public int getNumUnpinned() { return this.getNumBuffers() - this.getNumPinned(); }
}

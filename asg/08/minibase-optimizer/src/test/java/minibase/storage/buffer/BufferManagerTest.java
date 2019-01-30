/*
 * @(#)BufferManagerTest.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import minibase.BaseTest;
import minibase.TestHelper;

/**
 * Test suite for the buffer manager layer.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public class BufferManagerTest extends BaseTest {

   /** Does a simple test of normal buffer manager operations. */
   @Test
   public void normalOperations() {
      final BufferManager bufferManager = this.getBufferManager();
      // we choose this number to ensure that at least one page
      // will have to be written during this test
      assertEquals(0, bufferManager.getNumPinned());
      final int numPages = bufferManager.getNumUnpinned() + 1;

      // Allocate a bunch of new pages
      final PageID firstID = bufferManager.getDiskManager().allocatePages(numPages);

      // Write something on each one
      for (int page = 0; page < numPages; page++) {
         final PageID pid = PageID.getInstance(firstID.getValue() + page);
         final Page< ? > pg = bufferManager.pinPage(pid);

         // Copy the page number + 99999 onto each page.
         // It seems unlikely that this bit pattern would show up there by coincidence.
         final int data = pid.getValue() + 99999;
         pg.writeInt(0, data);

         bufferManager.unpinPage(pg, UnpinMode.DIRTY);
      }

      // Read that something back from each one (because we're buffering, this is where most
      // of the writes happen)
      for (int page = 0; page < numPages; page++) {
         final PageID pid = PageID.getInstance(firstID.getValue() + page);
         final Page< ? > pg = bufferManager.pinPage(pid);
         assertEquals(pg.readInt(0), pid.getValue() + 99999);
         bufferManager.unpinPage(pg, UnpinMode.CLEAN);
      }

      // Free the pages again
      for (int page = 0; page < numPages; page++) {
         final PageID pid = PageID.getInstance(firstID.getValue() + page);
         bufferManager.freePage(bufferManager.pinPage(pid));
      }
   }

   /** Exercises some illegal buffer manager operations. */
   @Test
   public void illegalOperations() {
      final BufferManager bufferManager = this.getBufferManager();
      assertEquals(0, bufferManager.getNumPinned());

      // we choose this number to ensure that pinning
      // this number of buffers should fail
      final int numPages = bufferManager.getNumUnpinned() + 1;

      // Try to pin more pages than there are frames
      final PageID firstPid = bufferManager.getDiskManager().allocatePages(numPages);
      final PageID lastPid = PageID.getInstance(firstPid.getValue() + numPages - 1);

      // first pin enough pages that there is no more room
      final Page< ? >[] pages = new Page< ? >[numPages - 1];
      for (int page = 0; page < numPages - 1; page++) {
         pages[page] = bufferManager.pinPage(PageID.getInstance(firstPid.getValue() + page));
      }

      // make sure the buffer manager thinks there's no more room
      assertEquals(0, bufferManager.getNumUnpinned());
      // now pin that last page, and make sure it fails
      TestHelper.assertThrows(IllegalStateException.class, () -> bufferManager.pinPage(lastPid));

      // Try to free a doubly-pinned page
      final Page< ? > first = bufferManager.pinPage(firstPid);
      TestHelper.assertThrows(IllegalArgumentException.class, () -> bufferManager.freePage(first));

      bufferManager.unpinPage(first, UnpinMode.CLEAN);

      for (final Page< ? > page : pages) {
         bufferManager.freePage(page);
      }
      bufferManager.freePage(bufferManager.pinPage(lastPid));
   }

   /** Exercises some of the internals of the buffer manager. */
   @Test
   public void internals() {
      final BufferManager bufferManager = this.getBufferManager();
      final int numPages = BaseTest.BUF_SIZE + 10;
      final PageID[] pids = new PageID[numPages];

      // Allocate and dirty some new pages, one at a time, and leave some pinned
      for (int index = 0; index < numPages; index++) {
         final Page< ? > page = bufferManager.newPage();
         pids[index] = page.getPageID();

         // Copy the page number + 99999 onto each page. It seems
         // unlikely that this bit pattern would show up there by
         // coincidence.
         page.writeInt(0, pids[index].getValue() + 99999);

         // Leave the page pinned if it equals 12 mod 20. This is a
         // random number based loosely on a bug report.
         if (pids[index].getValue() % 20 != 12) {
            bufferManager.unpinPage(page, UnpinMode.DIRTY);
         }
      }

      // Read the pages
      for (int index = 0; index < numPages; index++) {
         final PageID pid = pids[index];
         final Page< ? > page = bufferManager.pinPage(pid);
         assertEquals(pid.getValue() + 99999, page.readInt(0));

         if (pid.getValue() % 20 == 12) {
            bufferManager.unpinPage(page, UnpinMode.DIRTY);
         }

         // might not be dirty
         bufferManager.freePage(page);
      }
   }

   /** Tests that pinning a non-allocated page fails. */
   @Test
   public void pinAfterFree() {
      final BufferManager bufferManager = this.getBufferManager();

      // test with never-allocated page
      final PageID invalidID = PageID.getInstance(DB_SIZE - 1);
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> bufferManager.pinPage(invalidID));

      // test with previously de-allocated page
      final Page<?> page = bufferManager.newPage();
      final PageID pageID = page.getPageID();
      page.writeInt(0, 42);
      bufferManager.unpinPage(page, UnpinMode.DIRTY);

      final Page<?> page2 = bufferManager.pinPage(pageID);
      assertEquals(42, page2.readInt(0));
      bufferManager.freePage(page2);

      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> bufferManager.pinPage(pageID));
   }
}

/*
 * @(#)DiskManagerTest.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import minibase.BaseTest;
import minibase.TestHelper;
import minibase.storage.buffer.PageID;
import minibase.util.Convert;

/**
 * Test suite for the disk manager layer.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public class DiskManagerTest extends BaseTest {

   /** Identifies the first page in the test run of pages. */
   private PageID runStart = null;

   /** Creates a new database and does some tests of normal operations. */
   @Test
   public void normalOperations() {
      final DiskManager diskManager = this.getDiskManager();

      // Add some file entries
      for (int i = 0; i < 6; i++) {
         final String name = "file" + i;
         final PageID pgid = diskManager.allocatePage();
         diskManager.addFileEntry(name, pgid);
      }

      // Allocate a run of pages
      this.runStart = diskManager.allocatePages(30);

      // Write something on some of them
      for (int i = 0; i < 20; i++) {
         final String writeStr = "A" + i;
         // leave enough space
         final byte[] data = new byte[DiskManager.PAGE_SIZE];
         Convert.writeString(data, 0, writeStr, writeStr.length());
         diskManager.writePage(PageID.getInstance(this.runStart.getValue() + i), data);
      }

      // Deallocate some of them
      diskManager.deallocatePages(PageID.getInstance(this.runStart.getValue() + 20), 10);
   }

   /** Opens the database created in test 1 and does some further tests. */
   @Test
   public void furtherTests() {
      this.normalOperations();
      final DiskManager diskManager = this.getDiskManager();

      // Delete some of the file entries
      for (int i = 0; i < 3; i++) {
         diskManager.deleteFileEntry("file" + i);
      }

      // Look up file entries that should still be there
      for (int i = 3; i < 6; i++) {
         diskManager.getFileEntry("file" + i);
      }

      // Read stuff back from pages we wrote in test 1
      for (int i = 0; i < 20; i++) {
         final byte[] data = new byte[DiskManager.PAGE_SIZE];
         diskManager.readPage(PageID.getInstance(this.runStart.getValue() + i), data);

         final String testStr = "A" + i;
         assertEquals(testStr, Convert.readString(data, 0, 2 * testStr.length()));
      }

      // final cleaning up before we leave the test
      diskManager.deallocatePages(this.runStart, 26);
   }

   /** Tests for some error conditions. */
   @Test
   public void errorConditions() {
      this.furtherTests();
      final DiskManager diskManager = this.getDiskManager();

      // Look up a deleted file entry
      assertNull(diskManager.getFileEntry("file1"));

      // Try to delete a deleted entry again
      TestHelper.assertThrows(IllegalArgumentException.class, () -> diskManager.deleteFileEntry("file1"));

      // Try to delete a nonexistent file entry
      TestHelper.assertThrows(IllegalArgumentException.class, () -> diskManager.deleteFileEntry("blargle"));

      // Look up a nonexistent file entry
      assertNull(diskManager.getFileEntry("blargle"));

      // Try to add a file entry that's already there
      TestHelper.assertThrows(IllegalArgumentException.class, () -> diskManager.addFileEntry("file3", this.runStart));

      // create a byte array that is big enough to fail the test
      final char[] data = new char[DiskManager.NAME_MAXLEN + 5];
      for (int i = 0; i < data.length; i++) {
         data[i] = 'x';
      }

      // Try to add a file entry whose name is too long
      final String name = String.valueOf(data);
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> diskManager.addFileEntry(name, PageID.getInstance(0)));

      // Try to allocate a run of pages that's too long
      TestHelper.assertThrows(IllegalStateException.class, () -> diskManager.allocatePages(BaseTest.DB_SIZE));

      // Try to allocate a negative run of pages
      TestHelper.assertThrows(IllegalArgumentException.class, () -> diskManager.allocatePages(-10));

      // Try to deallocate a negative run of pages
      // made up 1 to test -10
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> diskManager.deallocatePages(PageID.getInstance(0), -10));
   }

   /** Tests some boundary conditions. These tests are very implementation-specific. */
   @Test
   public void boundaryConditions() {
      this.furtherTests();
      final DiskManager diskManager = this.getDiskManager();

      final int pid = diskManager.getAllocCount();

      // we create a new database that's big enough to require 2 pages to hold its space map

      System.out.println(diskManager.toString());
      // Allocate all pages remaining after DB overhead is accounted for
      assertEquals(pid, diskManager.allocatePages(BaseTest.DB_SIZE - pid).getValue());

      // Attempt to allocate one more page
      TestHelper.assertThrows(IllegalStateException.class, () -> diskManager.allocatePage());

      // Free some of the allocated pages
      diskManager.deallocatePages(PageID.getInstance(pid), 7);
      diskManager.deallocatePages(PageID.getInstance(pid + 30), 8);

      // Allocate some of the just-freed pages
      assertEquals(pid + 30, diskManager.allocatePages(8).getValue());

      // Free two continued run of the allocated pages
      diskManager.deallocatePages(PageID.getInstance(pid + 8), 7);
      diskManager.deallocatePages(PageID.getInstance(pid + 15), 11);

      // Allocate back number of pages equal to the just freed pages
      assertEquals(pid + 8, diskManager.allocatePages(18).getValue());

      // Delete some leftover file entries
      for (int i = 3; i < 6; i++) {
         diskManager.deleteFileEntry("file" + i);
      }

      // Add enough file entries that the directory must surpass a page

      // This over-counts, but uses only public info.
      final int count = DiskManager.PAGE_SIZE / DiskManager.NAME_MAXLEN + 1;

      for (int i = 0; i < count; i++) {
         // Set every file's first page to be page pid, which doesn't cause an error.
         diskManager.addFileEntry("file" + i, PageID.getInstance(pid));
      }

      // Make sure that the directory has taken up an extra page:
      // try to allocate more pages than should be available

      // There should only be 6 pages available.
      TestHelper.assertThrows(IllegalStateException.class, () -> diskManager.allocatePages(7));

      // Should work.
      diskManager.allocatePages(6);

      // At this point, all pages should be claimed. Try to allocate one more.
      TestHelper.assertThrows(IllegalStateException.class, () -> diskManager.allocatePage());

      // Free the last two pages: this tests a boundary condition in the space map.
      diskManager.deallocatePages(PageID.getInstance(BaseTest.DB_SIZE - 2), 2);
   }
}

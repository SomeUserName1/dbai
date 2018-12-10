/*
 * @(#)BucketPageTest.java   1.0   Oct 12, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.hash;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import minibase.BaseTest;
import minibase.RecordID;
import minibase.SearchKey;
import minibase.TestHelper;
import minibase.access.index.DataEntry;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;

/**
 * Tests the {@link BucketPage}.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 * @version 1.0
 */
public class BucketPageTest extends BaseTest {

   /** Instance of a page. */
   private Page<BucketPage> page;
   /** Dummy data entry for static use. */
   private static final DataEntry DUMMY = new DataEntry(new SearchKey(0),
         new RecordID(PageID.getInstance(0), 0), DataEntry.getLength(new SearchKey(0).getLength()));

   /**
    * Setup method.
    */
   @Before
   public void setUp() {
      this.page = BucketPage.newPage(this.getBufferManager());
   }

   /**
    * Tear down method.
    */
   @After
   public void tearDown() {
      this.getBufferManager().freePage(this.page);
   }

   /**
    * Tests that a bucket page is initialized correctly.
    */
   @Test
   public void testNew() {
      Assert.assertEquals(PageID.INVALID, BucketPage.getNextPagePointer(this.page));
      Assert.assertEquals(0, BucketPage.getNumEntries(this.page));
   }

   /**
    * Tests that appended data entries are at the position they are supposed to be.
    */
   @Test
   public void testAppend() {
      int num = 0;
      while (num < BucketPage.maxNumEntries(DUMMY.getEntrySize())) {
         final DataEntry e = this.randomEntry();
         BucketPage.appendDataEntry(this.page, e, DUMMY.getEntrySize());
         Assert.assertNotNull(BucketPage.readDataEntryAt(this.page, num, e.getEntrySize()));
         num++;
      }
      Assert.assertEquals(num, BucketPage.getNumEntries(this.page));
      Assert.assertFalse(BucketPage.hasSpaceLeft(this.page, DUMMY.getEntrySize()));
   }

   /**
    * Tests that we can find an appended data entry.
    */
   @Test
   public void testPresent() {
      final DataEntry e = this.randomEntry();
      BucketPage.appendDataEntry(this.page, e, DUMMY.getEntrySize());
      Assert.assertTrue(BucketPage.checkEntryPresent(this.page, e, DUMMY.getEntrySize()));
      Assert.assertFalse(BucketPage.checkEntryPresent(this.page, DUMMY, DUMMY.getEntrySize()));
   }

   /**
    * Tests that we can remove a data entry and that it's position is filled, i.e. the page is still tightly
    * packed.
    */
   @Test
   public void testRemove() {
      this.fillPage(this.page);
      final int num = BucketPage.getNumEntries(this.page);
      Assert.assertTrue(num > 0);
      for (int i = 0; i < num; ++i) {
         // first position gets refilled when deleting, so we can always delete at this position
         BucketPage.removeDataEntryAt(this.page, 0, DUMMY.getEntrySize());
      }
      Assert.assertEquals(0, BucketPage.getNumEntries(this.page));
      // nothing left to do as (re)moved entries are no longer invalidated
   }

   /**
    * Tests moving of entries on bucket pages.
    */
   @Test
   public void testMove() {
      final Page<BucketPage> dest = BucketPage.newPage(this.getBufferManager());
      this.fillPage(this.page);

      final int num = BucketPage.getNumEntries(this.page);
      for (int pos = num - 1; pos >= 0; --pos) {
         final DataEntry e = BucketPage.readDataEntryAt(this.page, pos, DUMMY.getEntrySize());
         assertNotNull(e);
         final int size = e.getEntrySize();
         assertNull(BucketPage.readDataEntryAt(dest, pos, size));

         BucketPage.moveLastEntryTo(this.page, dest, pos, size);

         assertNotNull(BucketPage.readDataEntryAt(dest, pos, size));
      }

      Assert.assertEquals(num, BucketPage.getNumEntries(dest));
      this.getBufferManager().freePage(dest);
   }

   /**
    * Tests the overflow pointing.
    */
   @Test
   public void testOverflowPointer() {
      final PageID pid = PageID.getInstance(1);
      BucketPage.setOverflowPointer(this.page, pid);
      Assert.assertEquals(pid, BucketPage.getNextPagePointer(this.page));
   }

   /**
    * Tests illegal arguments for operations on the bucket page.
    */
   @Test
   public void testIllegalArguments() {
      // non-negative position
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> BucketPage.removeDataEntryAt(this.page, -1, 1));
      // nothing to remove at position
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> BucketPage.removeDataEntryAt(this.page, 0, 1));

      // non-negative position
      TestHelper.assertThrows(IllegalArgumentException.class,
            () -> BucketPage.readDataEntryAt(this.page, -1, 1));
   }

   /**
    * Helper method for filling a page completely with random data entries.
    *
    * @param page
    *           page to fill
    */
   private void fillPage(final Page<BucketPage> page) {
      final DataEntry e = this.randomEntry();
      final int size = e.getEntrySize();
      final int num = BucketPage.maxNumEntries(size);
      for (int i = 0; i < num; i++) {
         BucketPage.appendDataEntry(page, this.randomEntry(), size);
      }
   }

   /**
    * Random data entry generator.
    *
    * @return a random data entry
    */
   private DataEntry randomEntry() {
      final int r = this.getRandom().nextInt();
      final SearchKey key = new SearchKey(r);
      return new DataEntry(key, new RecordID(PageID.getInstance(r), r), DataEntry.getLength(key.getLength()));
   }
}

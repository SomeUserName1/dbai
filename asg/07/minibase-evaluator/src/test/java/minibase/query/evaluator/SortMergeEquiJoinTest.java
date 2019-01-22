/*
 * @(#)SortMergeEquiJoinTest.java   1.0   Jan 22, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

import minibase.access.file.FileScan;
import minibase.access.file.HeapFile;
import minibase.util.Convert;

/**
 * @author Manuel Hotz &lt;manuel.hotz&gt;
 * @since 1.0
 */
public class SortMergeEquiJoinTest extends EvaluatorBaseTest {

   /**
    * Stupid test.
    */
   @Test
   public void testPrint() {
      // test join relation with empty relation
      try (HeapFile s = (HeapFile) createSailors(10000);
            HeapFile b = (HeapFile) createBoats(10000);
            HeapFile r = (HeapFile) createReserves(400, 400, 40)) {
         final FileScan fsS = s.openScan();
         final FileScan fsB = b.openScan();
         final FileScan fsR = r.openScan();

         System.out.println("======Input Relations =======");
         printSailor(fsS);
         printBoats(fsB);
         printReservations(fsR);

         fsS.close();
         fsB.close();
         fsR.close();

         final SortMergeEquiJoin smj = new SortMergeEquiJoin(this.getBufferManager(),
               new TableScan(S_SAILORS, s), 0, new TableScan(S_RESERVES, r), 0);
         try (TupleIterator ti = smj.open()) {
            final int count = this.printSailorSMJReservation(ti);
            System.out.println("# matches: " + count);
         }
      }
   }

   /**
    * Stupid test.
    */
   @Test
   public void testEmpty() {
      // test join relation with empty relation
      try (HeapFile f = HeapFile.createTemporary(this.getBufferManager());
            HeapFile o = HeapFile.createTemporary(this.getBufferManager())) {
         final byte[] data = S_SAILORS.newTuple();
         S_SAILORS.setAllFields(data, 1, "c", 1, 1.0f);
         f.insertRecord(data);

         final SortMergeEquiJoin smj = new SortMergeEquiJoin(this.getBufferManager(),
               new TableScan(S_SAILORS, f), 0, new TableScan(S_SAILORS, o), 0);
         try (TupleIterator it = smj.open()) {
            assertFalse(it.hasNext());
         }
      }
   }

   /**
    * Some stupid print function.
    *
    * @param ti tuple iterator to print
    * @return the number of matches
    */
   private int printSailorSMJReservation(final TupleIterator ti) {
      int count = 0;
      while (ti.hasNext()) {
         final byte[] nextTuple = ti.next();
         System.out.print(Convert.readInt(nextTuple, 0) + "\t");
         System.out.print(Convert.readString(nextTuple, 4, 50) + "  \t");
         System.out.print(Convert.readInt(nextTuple, 54) + "\t");
         System.out.print(Convert.readFloat(nextTuple, 58) + "\t");
         System.out.print(Convert.readInt(nextTuple, 62) + "\t");
         System.out.print(Convert.readInt(nextTuple, 66) + "\t");
         System.out.print(Convert.readDate(nextTuple, 70) + "\t");
         System.out.println(Convert.readString(nextTuple, 73, 50) + "  \t");
         count++;
      }
      return count;
   }
}







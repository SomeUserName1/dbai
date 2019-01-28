/*
 * @(#)JoinTest05.java   1.0   Jan 22, 2019
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
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import minibase.access.file.File;

import minibase.access.file.FileScan;
import minibase.access.file.HeapFile;
import minibase.util.Convert;

/**
 * @author Manuel Hotz &lt;manuel.hotz&gt;
 * @since 1.0
 */
public class JoinTest05 extends EvaluatorBaseTest05 {

   /**
    * Stupid test.
    */
   @Test
   public void testPrint() {
      // test join relation with empty relation
      try (HeapFile s = (HeapFile) createSailors(1000);
           HeapFile r = (HeapFile) createReserves(400, 1000, 1000)) {

          final FileScan fsS = s.openScan();
          final FileScan fsR = r.openScan();

         System.out.println("======Input Relations =======");
         printSailor(fsS);
         printReservations(fsR);

         fsS.close();
         fsR.close();

         final SortMergeEquiJoin05 smj = new SortMergeEquiJoin05(this.getBufferManager(),
               new TableScan(S_RESERVES, r), 0, new TableScan(S_SAILORS, s), 0);
         try (TupleIterator ti = smj.open()) {
            final int count = this.printReservationSMJSailors(ti);
            assertEquals(count, 400);
            System.out.println("# matches: " + count);
         } catch (final Exception e) {
            e.printStackTrace();
         }
      }
   }

   /**
    * some test.
    */
   @Test
   public void testInnerOuter() {
       try (HeapFile s = (HeapFile) createSailors(1000);
            HeapFile r = (HeapFile) createReserves(400, 1000, 1000)) {

           final SortMergeEquiJoin05 smj1 = new SortMergeEquiJoin05(this.getBufferManager(),
                   new TableScan(S_SAILORS, s), 0, new TableScan(S_RESERVES, r), 0);
           final SortMergeEquiJoin05 smj2 = new SortMergeEquiJoin05(this.getBufferManager(),
                   new TableScan(S_RESERVES, r), 0, new TableScan(S_SAILORS, s), 0);

           try (TupleIterator ti1 = smj1.open(); TupleIterator ti2 = smj2.open()) {
               assertEquals(this.countTuples(ti1), this.countTuples(ti2));
           } catch (final Exception e) {
               e.printStackTrace();
           }
       }
   }

   /**
    * some test.
    */
   @Test
   public void sidBid() {
       // test join relation with empty relation
       try (HeapFile s = (HeapFile) createSailors(1000);
            HeapFile r = (HeapFile) createReserves(400, 1000, 1000)) {

           final FileScan fsS = s.openScan();
           final FileScan fsR = r.openScan();

           System.out.println("======Input Relations =======");
           printSailor(fsS);
           printReservations(fsR);

           fsS.close();
           fsR.close();

           final SortMergeEquiJoin05 smj = new SortMergeEquiJoin05(this.getBufferManager(),
                   new TableScan(S_RESERVES, r), 1, new TableScan(S_SAILORS, s), 0);
           try (TupleIterator ti = smj.open()) {
               final int count = this.printReservationSMJSailors(ti);
               assertEquals(count, 400);
               System.out.println("# matches: " + count);
           } catch (final Exception e) {
               e.printStackTrace();
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

         final SortMergeEquiJoin05 smj = new SortMergeEquiJoin05(this.getBufferManager(),
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
          System.out.print(Convert.readString(nextTuple, 4, 50) + "\t");
          System.out.print(Convert.readInt(nextTuple, 54) + "\t");
          System.out.println(Convert.readFloat(nextTuple, 58) + "  \t");

          System.out.print(Convert.readInt(nextTuple, 62) + "\t");
          System.out.print(Convert.readInt(nextTuple, 64) + "  \t");
          System.out.print(Convert.readDate(nextTuple, 68) + "\t");
          System.out.print(Convert.readString(nextTuple, 71, 50) + "\t");
         count++;
      }
      return count;
   }

   /**
    * print function.
    * @param ti tuple iterator to print
    * @return the number of tuples
    */
    private int printReservationSMJSailors(final TupleIterator ti) {
        int count = 0;
        while (ti.hasNext()) {
            final byte[] nextTuple = ti.next();
            System.out.print(Convert.readInt(nextTuple, 0) + "\t");
            System.out.print(Convert.readInt(nextTuple, 4) + "  \t");
            System.out.print(Convert.readDate(nextTuple, 8) + "\t");
            System.out.print(Convert.readString(nextTuple, 11, 50) + "\t");


            System.out.print(Convert.readInt(nextTuple, 61) + "\t");
            System.out.print(Convert.readString(nextTuple, 65, 50) + "\t");
            System.out.print(Convert.readInt(nextTuple, 115) + "\t");
            System.out.println(Convert.readFloat(nextTuple, 119) + "  \t");
            count++;
        }
        return count;
    }

    /**
     * Count the number of return tupels.
     *
     * @param ti tuple iterator to count
     * @return number of tupels
     */
    private int countTuples(final TupleIterator ti) {
       int count = 0;
       while (ti.hasNext()) {
           ti.next();
           count++;
       }
       return count;
    };


    /**
     * Returns the heap file containing the data of the Sailors relation.
     * @param num number of tuples to generate
     *
     * @return heap file of the Sailors relation
     */
    protected File createSailorsCross(final int num) {
        final File sailors = HeapFile.createTemporary(this.getBufferManager());
        for (int i = 0; i < num; i++) {
            final byte[] tuple = EvaluatorBaseTest05.S_SAILORS.newTuple();
            S_SAILORS.setAllFields(tuple,
                    0,
                    EvaluatorBaseTest05.SNAMES[this.getRandom().nextInt(EvaluatorBaseTest05.SNAMES.length)],
                    this.getRandom().nextInt(11),
                    this.getRandom().nextFloat() * 81.9f + 18.0f
            );
            sailors.insertRecord(tuple);
        }
        return sailors;
    }
}







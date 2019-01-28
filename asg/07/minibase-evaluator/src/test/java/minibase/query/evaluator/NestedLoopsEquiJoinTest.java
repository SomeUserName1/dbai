/*
 * @(#)NestedLoopsEquiJoinTest.java   1.0   Jan 22, 2019
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

import minibase.access.file.HeapFile;

/**
 * @author Manuel Hotz &lt;manuel.hotz&gt;
 * @since 1.0
 */
public class NestedLoopsEquiJoinTest extends EvaluatorBaseTest05 {

   /**
    * some test.
    */
   @Test
   public void testEmpty() {
      // test join relation with empty relation
      try (HeapFile f = HeapFile.createTemporary(this.getBufferManager());
           HeapFile o = HeapFile.createTemporary(this.getBufferManager())) {
         final byte[] data = S_SAILORS.newTuple();
         S_SAILORS.setAllFields(data, 1, "c", 1, 1.0f);
         f.insertRecord(data);

         final NestedLoopsEquiJoin nlj = new NestedLoopsEquiJoin(this.getBufferManager(),
                 new TableScan(S_SAILORS, f), 0, new TableScan(S_SAILORS, o), 0);
         try (TupleIterator it = nlj.open()) {
            assertFalse(it.hasNext());
         }
      }
   }
}

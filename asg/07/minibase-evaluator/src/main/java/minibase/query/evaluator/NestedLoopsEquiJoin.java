/*
 * @(#)NestedLoopsEquiJoin.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.NoSuchElementException;

import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.evaluator.compare.TupleComparator;
import minibase.query.schema.Schema;
import minibase.storage.buffer.BufferManager;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition, section 14.4.1, page 454).
 *
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 */
public final class NestedLoopsEquiJoin extends AbstractOperator {

   /** Outer input relation. */
   private final Operator outer;

   /** Inner input relation. */
   private final Operator inner;

   /** Join predicate. */
   private final RecordComparator predicate;

   /**
    * Constructs a join, given the left and right inputs and the offsets of the columns to compare.
    *
    * @param bufferManager buffer manager, unused for this join
    * @param outer outer relation
    * @param outerColumn column of the outer relation to compare
    * @param inner inner relation
    * @param innerColumn column of the inner relation to compare
    */
   public NestedLoopsEquiJoin(final BufferManager bufferManager, final Operator outer, final int outerColumn,
         final Operator inner, final int innerColumn) {
      super(Schema.join(outer.getSchema(), inner.getSchema()));
      this.outer = outer;
      this.inner = inner;
      this.predicate = new TupleComparator(outer.getSchema(), new int[] { outerColumn },
            inner.getSchema(), new int[] { innerColumn });
   }

   @Override
   public TupleIterator open() {
      final TupleIterator outer = this.outer.open();
      if (!outer.hasNext()) {
         outer.close();
         return TupleIterator.EMPTY;
      }
      final TupleIterator inner = this.inner.open();
      if (!inner.hasNext()) {
         outer.close();
         inner.close();
         return TupleIterator.EMPTY;
      }

      return new TupleIterator() {

         /** Current tuple from the iterator of the outer relation. */
         private byte[] currentOuter = outer.next();

         /** Next tuple to return. */
         private byte[] next;

         @Override
         public boolean hasNext() {
            if (this.next != null) {
               return true;
            }

            for (;;) {
               if (this.currentOuter != null) {
                  while (inner.hasNext()) {
                     final byte[] currentInner = inner.next();
                     if (NestedLoopsEquiJoin.this.predicate.equals(this.currentOuter, currentInner)) {
                        this.next = Schema.join(this.currentOuter, currentInner);
                        return true;
                     }
                  }
                  this.currentOuter = null;
               }
               if (!outer.hasNext()) {
                  return false;
               }
               this.currentOuter = outer.next();
               inner.reset();
            }
         }

         @Override
         public byte[] next() {
            // validate the next tuple
            if (!this.hasNext()) {
               throw new NoSuchElementException("No more tuples to return.");
            }
            // return (and forget) the tuple
            final byte[] tuple = this.next;
            this.next = null;
            return tuple;
         }

         @Override
         public void reset() {
            outer.reset();
            inner.reset();
            this.currentOuter = null;
            this.next = null;
         }

         @Override
         public void close() {
            outer.close();
            inner.close();
            this.currentOuter = null;
            this.next = null;
         }
      };
   }
}

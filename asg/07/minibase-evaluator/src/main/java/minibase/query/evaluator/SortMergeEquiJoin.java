/*
 * @(#)SortMergeEquiJoin.java   1.0   Jan 22, 2019
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
import java.util.concurrent.ConcurrentLinkedQueue;

import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.evaluator.compare.TupleComparator;
import minibase.query.schema.Schema;
import minibase.storage.buffer.BufferManager;

/**
 * The Sort Merge Join as described in the lecture slides 07-relopseval 24 ff.
 * and 452 in the book.
 *
 * @author fabian.klopfer@uni-konstanz.de, simon.suckut@uni-konstanz.de
 */
public class SortMergeEquiJoin extends AbstractOperator {

   /**
    * buffermanager to allocate output space and sort pages with.
    */
   private final BufferManager bufferManager;

   /**
    * Outer input relation.
    */
   private final Operator outer;

   /**
    * Inner input relation.
    */
   private final Operator inner;

   /**
    * Join predicate.
    */
   private final RecordComparator predicate;

   /**
    * Constructs a join, given the left and right inputs and the offsets of the columns to compare.
    *
    * @param bufferManager buffer manager, unused for this join
    * @param outer         outer relation
    * @param outerColumn   column of the outer relation to compare
    * @param inner         inner relation
    * @param innerColumn   column of the inner relation to compare
    */
   SortMergeEquiJoin(final BufferManager bufferManager, final Operator outer, final int outerColumn,
         final Operator inner, final int innerColumn) {
      super(Schema.join(outer.getSchema(), inner.getSchema()));
      this.bufferManager = bufferManager;
      this.outer = outer;
      this.inner = inner;
      this.predicate = new TupleComparator(outer.getSchema(), new int[] { outerColumn }, inner.getSchema(),
            new int[] { innerColumn });
   }

   @Override public TupleIterator open() {
      final TupleIterator outerSorted = new ExternalSort(this.bufferManager, this.outer, this.predicate)
            .open();
      if (!outerSorted.hasNext()) {
         outerSorted.close();
         return TupleIterator.EMPTY;
      }
      final TupleIterator innerSorted = new ExternalSort(this.bufferManager, this.inner, this.predicate)
            .open();
      if (!innerSorted.hasNext()) {
         outerSorted.close();
         innerSorted.close();
         return TupleIterator.EMPTY;
      }

      return new TupleIterator() {

         /**
          * current outer tuple
          */
         private byte[] outerTuple = outerSorted.next();

         /**
          * current outer tuple
          */
         private byte[] innerTuple = innerSorted.next();

         /**
          * Next tuple to return.
          */
         private ConcurrentLinkedQueue<byte[]> nextBlock = new ConcurrentLinkedQueue<>();

         @Override public boolean hasNext() {
            if (!this.nextBlock.isEmpty()) {
               return true;
            }
            if (this.innerTuple == null || this.outerTuple == null) {
               return false;
            }
            while (SortMergeEquiJoin.this.predicate.lessThan(this.outerTuple, this.innerTuple)) {
               if (!outerSorted.hasNext()) {
                  return false;
               } else {
                  this.outerTuple = outerSorted.next();
                  // System.out.println("outerSorted.next()");
               }
            }
            while (SortMergeEquiJoin.this.predicate.greaterThan(this.outerTuple, this.innerTuple)) {
               if (!innerSorted.hasNext()) {
                  return false;
               } else {
                  this.innerTuple = innerSorted.next();
                  // System.out.println("innerSorted.next()");
               }
            }

            final byte[] prevInner = this.innerTuple;
            while (SortMergeEquiJoin.this.predicate.equals(this.outerTuple, prevInner)) {
               innerTuple = prevInner;
               while (SortMergeEquiJoin.this.predicate.equals(this.outerTuple, this.innerTuple)) {
                  // System.out.println("adding joined tuple");
                  this.nextBlock.add(Schema.join(this.outerTuple, this.innerTuple));

                  if (!innerSorted.hasNext()) {
                     // System.out.println("inner relation empty return true");
                     this.innerTuple = null;
                     return true;
                  } else {
                     // System.out.println("increment inner sorted");
                     this.innerTuple = innerSorted.next();
                  }
               }
               if (!outerSorted.hasNext()) {
                  // System.out.println("outer sorted has no next after matchting loop. ret if sth was "
                  //     + "found");
                  this.outerTuple = null;
                  return !this.nextBlock.isEmpty();
               } else {
                  // System.out.println("else increment outer sorted");
                  this.outerTuple = outerSorted.next();
               }
               // System.out.println("end first equals => require true");
            }

            return !this.nextBlock.isEmpty();
         }

         @Override public byte[] next() {
            // validate the next tuple
            if (!this.hasNext()) {
               this.close();
               throw new NoSuchElementException("No more tuples to return.");
            }
            // return (and forget) the tuple
            return this.nextBlock.poll();
         }

         @Override public void reset() {
            outerTuple = null;
            innerTuple = null;
            this.nextBlock.clear();
            outerSorted.reset();
            innerSorted.reset();
         }

         @Override public void close() {
            outerTuple = null;
            innerTuple = null;
            this.nextBlock.clear();
            outerSorted.close();
            innerSorted.close();
         }
      };
   }
}

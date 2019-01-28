/*
 * @(#)SortMergeEquiJoin05.java   1.0   Jan 22, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.LinkedList;
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
public class SortMergeEquiJoin05 extends AbstractOperator {

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

   private int innerColumn;
   private int outerColumn;

   /**
    * Constructs a join, given the left and right inputs and the offsets of the columns to compare.
    *
    * @param bufferManager buffer manager, unused for this join
    * @param outer         outer relation
    * @param outerColumn   column of the outer relation to compare
    * @param inner         inner relation
    * @param innerColumn   column of the inner relation to compare
    */
   SortMergeEquiJoin05(final BufferManager bufferManager, final Operator outer, final int outerColumn,
                       final Operator inner, final int innerColumn) {
      super(Schema.join(outer.getSchema(), inner.getSchema()));
      this.bufferManager = bufferManager;
      this.outer = outer;
      this.inner = inner;
      this.innerColumn = innerColumn;
      this.outerColumn = outerColumn;
      this.predicate = new TupleComparator(outer.getSchema(), new int[] { outerColumn }, inner.getSchema(),
            new int[] { innerColumn });
   }

   @Override
   public TupleIterator open() {

      final TupleIterator outerSorted = new ExternalSort(bufferManager, this.outer,
              new TupleComparator(this.outer.getSchema(), new int[] {outerColumn}, new boolean[] { true }), 5, 2).open();
      if (!outerSorted.hasNext()) {
         outerSorted.close();
         return TupleIterator.EMPTY;
      }

      final TupleIterator innerSorted = new ExternalSort(this.bufferManager, this.inner,
              new TupleComparator(this.inner.getSchema(), new int[] {innerColumn}, new boolean[] { true }), 5, 2).open();
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
            while (SortMergeEquiJoin05.this.predicate.lessThan(this.outerTuple, this.innerTuple)) {
               if (!outerSorted.hasNext()) {
                  return false;
               } else {
                  this.outerTuple = outerSorted.next();
               }
            }
            while (SortMergeEquiJoin05.this.predicate.greaterThan(this.outerTuple, this.innerTuple)) {
               if (!innerSorted.hasNext()) {
                  return false;
               } else {
                  this.innerTuple = innerSorted.next();
               }
            }

            final byte[] prevInner = innerTuple.clone();
            LinkedList<byte[]> innerList = new LinkedList<byte[]>();

            while (SortMergeEquiJoin05.this.predicate.equals(this.outerTuple, prevInner)) {

               innerTuple = prevInner;

               while (SortMergeEquiJoin05.this.predicate.equals(this.outerTuple, this.innerTuple)) {
                  // System.out.println("adding joined tuple");
                  this.nextBlock.add(Schema.join(this.outerTuple, this.innerTuple));
                  innerList.addLast(this.innerTuple);

                  if (!innerSorted.hasNext()) {
                     this.innerTuple = null;
                     return true;
                  } else {
                     this.innerTuple = innerSorted.next();
                  }
               }
               if (!outerSorted.hasNext()) {
                  this.outerTuple = null;
                  return !this.nextBlock.isEmpty();
               } else {
                  this.outerTuple = outerSorted.next();
                  while (outerSorted.hasNext() && SortMergeEquiJoin05.this.predicate.equals(this.outerTuple, prevInner)) {
                     for (byte[] t : innerList) {
                        this.nextBlock.add(Schema.join(this.outerTuple, t));
                        this.outerTuple = outerSorted.next();
                     }
                  }
                  innerList.clear();
               }
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

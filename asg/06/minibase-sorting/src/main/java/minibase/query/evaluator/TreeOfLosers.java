/*
 * @(#)TreeOfLosers.java   1.0   Dec 18, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.ArrayList;

import minibase.access.file.Run;
import minibase.access.file.RunPage;
import minibase.access.file.RunScan;
import minibase.query.evaluator.compare.RecordComparator;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;

/**
 * @author Fabian Klopfer, Simon Suckut
 */
public class TreeOfLosers implements TupleIterator {

   /** The runs pre sorted by ExternalSort. Each run containing one page of sorted tuples */
   private final ArrayList<TupleIterator> runsScan0;

   /** List to close all runs after the sorting is finished. */
   private final ArrayList<Run> runs0;

   /** True when the object is invalidated and therefore no more methods can be called. */
   private boolean isInvalidated;

   /** root of the tree of losers. */
   private TupleIterator root;

   /** Used to compare the records by. */
   private RecordComparator recordComparator;
   /** The BufferManager that is currently being used. */
   private BufferManager bufferManager;

   /**
    * Tree of losers creation.
    * 
    * @param runs0
    *           presorted participant pools
    * @param recordComparator
    *           referee used for battles
    * @param recordLength
    *           the length of a record
    * @param bufferManager
    *           the used buffermanager
    */
   TreeOfLosers(final ArrayList<Run> runs0, final RecordComparator recordComparator, final int recordLength,
         final BufferManager bufferManager) {
      this.runs0 = runs0;
      this.runsScan0 = new ArrayList<TupleIterator>();
      this.bufferManager = bufferManager;
      for (Run run : runs0) {
         this.runsScan0
               .add(new RunScan(this.bufferManager, run.getFirstPageID(), run.getLength(), recordLength));
      }
      this.isInvalidated = false;
      this.recordComparator = recordComparator;

      this.reset();
   }

   @Override
   public boolean hasNext() {
      this.invalidated();
      return this.root.hasNext();
   }

   @Override
   public byte[] next() {
      this.invalidated();
      return this.root.next();
   }

   @Override
   public void reset() {
      this.invalidated();
      for (TupleIterator runScan : this.runsScan0) {
         runScan.reset();
      }

      // build tree of losers
      TupleIterator[] layer = this.runsScan0.toArray(new TupleIterator[this.runsScan0.size()]);
      while (layer.length > 1) {
         final TupleIterator[] nextLayer = new TupleIterator[layer.length / 2];
         for (int i = 0; i < layer.length / 2; i++) {
            nextLayer[i] = new LoserNode(layer[i * 2], layer[i * 2 + 1], this.recordComparator);
         }
         if (layer.length % 2 == 1) {
            final int position = nextLayer.length - 1;
            nextLayer[position] = new LoserNode(nextLayer[position], layer[layer.length - 1],
                  this.recordComparator);
         }
         layer = nextLayer;
      }
      this.root = layer[0];
   }

   @Override
   public void close() {
      this.invalidated();
      this.isInvalidated = true;
      for (TupleIterator runScan : this.runsScan0) {
         runScan.close();
      }
      for (Run run : this.runs0) {
         // run.dealloc();
         Page<RunPage> currentPage = this.bufferManager.pinPage(run.getFirstPageID());
         PageID nextPageID = RunPage.getNextPage(currentPage);
         this.bufferManager.freePage(currentPage);
         while (!nextPageID.equals(PageID.INVALID)) {
            currentPage = this.bufferManager.pinPage(nextPageID);
            nextPageID = RunPage.getNextPage(currentPage);
            this.bufferManager.freePage(currentPage);
         }
      }
   }

   /**
    * Returns true if this scan was not closed before. Otherwise false.
    */
   private void invalidated() {
      if (this.isInvalidated) {
         throw new IllegalStateException("This run scan was already closed");
      }
   }

   /**
    * Return all runs for Testing purposes.
    * 
    * @return all runs
    */
   public ArrayList<Run> getRuns0() {
      return this.runs0;
   }

   /**
    * @author Fabian Klopfer, Simon Suckut
    */
   private class LoserNode implements TupleIterator {

      /** the loser of the last battle. */
      private byte[] loser = null;
      /** indicates whether the loser comes from 0=left or 1=right. */
      private boolean origin;

      /** the comparator used to compare the records aka the referee of the battle. */
      private final RecordComparator recordComparator;

      /** left pool of participants. */
      private TupleIterator left;
      /** right pool of participants. */
      private TupleIterator right;

      /**
       * Constructor.
       * 
       * @param left
       *           the left pool of participants
       * @param right
       *           the right pool of participants
       * @param comparator
       *           the referee
       */
      LoserNode(final TupleIterator left, final TupleIterator right, final RecordComparator comparator) {
         this.left = left;
         this.right = right;
         this.recordComparator = comparator;
      }

      @Override
      public boolean hasNext() {
         // TODO implement TupleIterator.hasNext
         if (this.loser == null) {
             return this.left.hasNext() || this.right.hasNext();
         }
         return true;
      }

      @Override
      public byte[] next() {
         // if (!this.hasNext()) {
         // throw new IllegalStateException("next() invoked in empty LoserNode data structure.");
         // }
         if (this.loser == null) {
            // no battle has been fought
            if (!this.left.hasNext()) {
               // left is empty, take element from right
               // FIXME name
               final byte[] arrayReturn = this.right.next();
               // take another element, if possible, to populate value
               if (this.right.hasNext()) {
                  this.loser = this.right.next();
               }
               this.origin = true;
               return arrayReturn;
            } else if (!this.right.hasNext()) {
               // right is empty, take element from left
               final byte[] arrayReturn = this.left.next();
               // take another element, if possible, to populate value
               if (this.left.hasNext()) {
                  this.loser = this.left.next();
               }
               this.origin = false;
               return arrayReturn;
            } else {
               // fight battle between left and right
               final byte[] leftElement = this.left.next();
               final byte[] rightElement = this.right.next();
               if (this.recordComparator.lessThan(leftElement, rightElement)) {
                  // left has won
                  this.loser = rightElement;
                  this.origin = true;
                  return leftElement;
               } else {
                  // right has won
                  this.loser = leftElement;
                  this.origin = false;
                  return rightElement;
               }
            }
         } else {
            // battle has already been fought
            if (this.origin) {
               if (this.left.hasNext()) {
                  // loser is from right, so take element from left to battle with
                  final byte[] element = this.left.next();
                  if (this.recordComparator.lessThan(element, this.loser)) {
                     return element;
                  } else {
                     final byte[] oldLoser = this.loser;
                     this.loser = element;
                     this.origin = false;
                     return oldLoser;
                  }
               } else {
                  // loser is from right, but left is empty
                  // pass current value upwards and take new element from right
                  final byte[] oldLoser = this.loser;
                  if (this.right.hasNext()) {
                     this.loser = this.right.next();
                     this.origin = true;
                  } else {
                     this.loser = null;
                  }
                  return oldLoser;
               }
            } else {
               if (this.right.hasNext()) {
                  // loser is from left, so battle with right
                  final byte[] element = this.right.next();
                  if (this.recordComparator.lessThan(this.loser, element)) {
                     final byte[] oldLoser = this.loser;
                     this.loser = element;
                     this.origin = true;
                     return oldLoser;
                  } else {
                     return element;
                  }
               } else {
                  // loser is from left, but no right participant
                  // pass current value upwards and take new element form left
                  final byte[] oldLoser = this.loser;
                  if (this.left.hasNext()) {
                     this.loser = this.left.next();
                     this.origin = false;
                  } else {
                     this.loser = null;
                  }
                  return oldLoser;
               }
            }
         }
      }

      @Override
      public void reset() {
         throw new UnsupportedOperationException("Called reset on LoserNode.");
      }

      @Override
      public void close() {
         throw new UnsupportedOperationException("Called close on LoserNode.");
      }
   }
}

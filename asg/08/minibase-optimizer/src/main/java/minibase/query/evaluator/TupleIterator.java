/*
 * @(#)TupleIterator.java   1.0   Jan 13, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Volcano-style iterator interface that is implemented by all operators in Minibase.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public interface TupleIterator extends Iterator<byte[]>, AutoCloseable {
   /** The empty iterator. */
   TupleIterator EMPTY = new TupleIterator() {
      @Override
      public boolean hasNext() {
         return false;
      }

      @Override
      public byte[] next() {
         throw new NoSuchElementException();
      }

      @Override
      public void reset() {
      }

      @Override
      public void close() {
      }
   };

   /**
    * Returns {@code true} if there are more tuples and {@code false} otherwise.
    *
    * @return {@code true} if there are more tuples available, {@code false} otherwise
    */
   @Override
   boolean hasNext();

   /**
    * Computes and returns the next tuple produced by this iterator. If there are no more tuples to return,
    * this method will throw a {@code NoSuchElementException}.
    *
    * @return next tuple
    * @throws NoSuchElementException
    *            if there are no more tuples to return
    */
   @Override
   byte[] next();

   /**
    * Resets this iterator to start at the first tuple again.
    */
   void reset();

   /**
    * Closes the iterator and releases all allocated resources, e.g., pinned pages.
    */
   @Override
   void close();
}

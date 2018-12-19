/*
 * @(#)PeekableIterator.java   1.0   Jun 08, 2016
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

/**
 * An iterator which provides a method to get the next item to be returned by a call to {@link #next()}.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class PeekableIterator implements TupleIterator {
   /** Input iterator. */
   private final TupleIterator input;

   /** Cached value. */
   private byte[] cached = null;

   /**
    * Constructs a new iterator from the given one.
    * @param input input iterator
    */
   public PeekableIterator(final TupleIterator input) {
      this.input = input;
   }

   /**
    * Returns the next element that would be returned by {@link #next()} without advancing this iterator.
    *
    * @return next element
    * @throws NoSuchElementException if this iterator is drained
    */
   public byte[] peek() {
      if (this.cached == null) {
         this.cached = this.input.next();
      }
      return this.cached;
   }

   @Override
   public boolean hasNext() {
      return this.cached != null || this.input.hasNext();
   }

   @Override
   public byte[] next() {
      if (this.cached != null) {
         final byte[] out = this.cached;
         this.cached = null;
         return out;
      }
      if (!this.input.hasNext()) {
         throw new NoSuchElementException();
      }
      return this.input.next();
   }

   @Override
   public void reset() {
      this.cached = null;
      this.input.reset();
   }

   @Override
   public void close() {
      this.cached = null;
      this.input.close();
   }
}

/*
 * @(#)LRUPolicy.java   1.0   Oct 11, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer.policy;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;

/**
 * A replacement policy that implements the <em>LRU</em> algorithm. The LRU policy evicts the
 * memory page (frame) that is least recently used.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public class LRUPolicy implements ReplacementPolicy {

   /** Set of indexes of currently pinned pages. */
   private final BitSet pinned = new BitSet();

   /** Queue to manage the LRU strategy of this replacement policy. */
   private final Deque<Integer> queue;

   /**
    * Constructs a LRU replacement policy.
    *
    * @param numBuffers
    *           size of the buffer pool managed by this buffer policy
    */
   public LRUPolicy(final int numBuffers) {
      this.queue = new ArrayDeque<>(numBuffers);
      // add buffer pages in ascending order into the queue
      for (int i = 0; i < numBuffers; i++) {
         // add all pages as none are pinned yet
         this.queue.add(i);
      }
   }

   @Override
   public void stateChanged(final int pos, final PageState newState) {
      switch (newState) {
         case FREE:
            // nothing to do
            break;
         case PINNED:
            this.pinned.set(pos);
            break;
         case UNPINNED:
         default:
            // move unpinned page to the tail of the queue
            this.pinned.clear(pos);
            this.queue.remove(pos);
            this.queue.add(pos);
            break;
      }
   }

   @Override
   public int pickVictim() {
      // find first page in the queue with pin count zero
      final Iterator<Integer> iter = this.queue.iterator();
      while (iter.hasNext()) {
         final int pos = iter.next();
         if (!this.pinned.get(pos)) {
            // move page to the tail of the queue
            iter.remove();
            this.queue.add(pos);
            // return the index of the page
            return pos;
         }
      }
      // error state if the clock can not identify a victim
      return -1;
   }
}

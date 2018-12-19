/*
 * @(#)MRUPolicy.java   1.0   Oct 11, 2013
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
 * A replacement policy that implements the <em>MRU</em> algorithm. The MRU policy evicts the
 * memory page (frame) that is most recently used.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public class MRUPolicy implements ReplacementPolicy {
   /** Set of indexes of currently pinned pages. */
   private final BitSet pinned = new BitSet();
   /** Stack of buffer pages. */
   private final Deque<Integer> stack;

   /**
    * Constructs a MRU replacement policy.
    *
    * @param numBuffers
    *           size of the buffer pool managed by this buffer policy
    */
   public MRUPolicy(final int numBuffers) {
      this.stack = new ArrayDeque<>(numBuffers);
      // insert buffer pages in reverse order into the stack
      for (int i = 0; i < numBuffers; i++) {
         // add all pages as none are pinned yet
         this.stack.push(i);
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
            // move unpinned page to the top of the stack
            this.pinned.clear(pos);
            this.stack.remove(pos);
            this.stack.push(pos);
            break;
      }
   }

   @Override
   public int pickVictim() {
      // find first page in the stack with pin count zero
      final Iterator<Integer> iter = this.stack.iterator();
      while (iter.hasNext()) {
         final int pos = iter.next();
         if (!this.pinned.get(pos)) {
            // move page to the top of the stack
            iter.remove();
            this.stack.push(pos);
            // return the index of the page
            return pos;
         }
      }
      // error state if the clock can not identify a victim
      return -1;
   }
}

/*
 * @(#)MRUPolicy.java   1.0   Oct 11, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer.policy;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

/**
 * A replacement policy that implements the <em>MRU</em> algorithm. The MRU policy
 * evicts a free (whenever possible) or the most recently used memory page (buffer page).
 *
 * @author Simon Suckut, Fabian Klopfer
 * @version 1.0
 */
public class MRUPolicyGroup00 implements ReplacementPolicy {

   /** Stack of unpinned pages. */
   private Stack<Integer> unpinnedStack;
   /** Set of fee pages. */
   private Set<Integer> freeSet;

   /**
    * Constructs a MRU ReplacementPolicy.
    * 
    * @param numBuffers size of the buffer pool managed by this buffer policy
    */
   public MRUPolicyGroup00(final int numBuffers) {
      this.unpinnedStack = new Stack<Integer>();
      this.freeSet = new HashSet<Integer>(numBuffers);
      for (int i = 0; i < numBuffers; i++) {
         this.freeSet.add(i);
      }
   }

   @Override
   public void stateChanged(final int pos, final PageState newState) {

      // Changes from FREE to UNPINNED are never needed so they are not covered by this method
      switch (newState) {
         case FREE:
            this.unpinnedStack.remove((Integer) pos);
            this.freeSet.add(pos);
            break;
         case PINNED:
            if (!this.freeSet.remove(pos)) {
               this.unpinnedStack.remove((Integer) pos);
            }
            break;
         case UNPINNED:
            this.unpinnedStack.add(pos);
            break;
         default:
            break;
      }
   }

   @Override
   public int pickVictim() {
      if (!this.freeSet.isEmpty()) {
         return this.freeSet.iterator().next();
      } else if (!this.unpinnedStack.isEmpty()) {
         return this.unpinnedStack.peek();
      }
      return -1;
   }
}

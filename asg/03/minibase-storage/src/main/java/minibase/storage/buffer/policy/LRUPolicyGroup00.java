/*
 * @(#)LRUPolicy.java   1.0   Oct 11, 2013
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
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * A replacement policy that implements the <em>LRU</em> algorithm. The LRU policy
 * evicts a free (whenever possible) or the least recently used memory page (buffer page).
 *
 * @author Simon Suckut, Fabian Klopfer
 * @version 1.0
 */
public class LRUPolicyGroup00 implements ReplacementPolicy {

   /** Queue of unpinned pages. */
   private Queue<Integer> unpinnedQueue;
   /** Set of free pages. */
   private Set<Integer> freeSet;
   
   /**
    * Constructs a LRU ReplacementPolicy.
    * 
    * @param numBuffers size of the buffer pool managed by this buffer policy
    */
   public LRUPolicyGroup00(final int numBuffers) {
      this.unpinnedQueue = new LinkedList<Integer>();
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
            this.unpinnedQueue.remove(pos);
            this.freeSet.add(pos);
            break;
         case PINNED:
            if (!this.freeSet.remove(pos)) {
               this.unpinnedQueue.remove(pos);
            }
            break;
         case UNPINNED:
            this.unpinnedQueue.add(pos);
            break;
         default:
            break;
      }
   }

   @Override
   public int pickVictim() {
      if (!this.freeSet.isEmpty()) {
         return this.freeSet.iterator().next();
      } else if (!this.unpinnedQueue.isEmpty()) {
         return this.unpinnedQueue.peek();
      }
      return -1;
   }
}

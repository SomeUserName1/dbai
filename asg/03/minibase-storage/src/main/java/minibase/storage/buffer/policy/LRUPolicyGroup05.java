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

import java.util.LinkedList;
import java.util.Queue;

/**
 * A replacement policy that implements the <em>LRU</em> algorithm. The LRU policy
 * evicts a free (whenever possible) or the least recently used memory page (buffer page).
 *
 * @author Simon Suckut, Fabian Klopfer
 * @version 1.0
 */
public class LRUPolicyGroup05 implements ReplacementPolicy {

   /** Queue of unpinned pages. */
   private Queue<Integer> unpinnedQueue;
   
   /**
    * Constructs a LRU ReplacementPolicy.
    * 
    * @param numBuffers size of the buffer pool managed by this buffer policy
    */
   public LRUPolicyGroup05(final int numBuffers) {
      this.unpinnedQueue = new LinkedList<Integer>();
      for (int i = 0; i < numBuffers; i++) {
         this.unpinnedQueue.add(i);
      }
   }

   @Override
   public void stateChanged(final int pos, final PageState newState) {
      
      switch (newState) {
         case FREE:
            // no need to update queue
            break;
         case PINNED:
            this.unpinnedQueue.remove(pos);
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
      if (!this.unpinnedQueue.isEmpty()) {
         return this.unpinnedQueue.peek();
      }
      return -1;
   }
}

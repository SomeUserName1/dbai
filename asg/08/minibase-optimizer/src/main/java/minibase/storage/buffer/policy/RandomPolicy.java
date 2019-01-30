/*
 * @(#)RandomPolicy.java   1.0   Oct 11, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A replacement policy that implements the <em>random</em> algorithm. The random policy
 * evicts a random memory page (buffer page).
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public class RandomPolicy implements ReplacementPolicy {
   /** Random number generator for this strategy. */
   private final Random random;
   /** List of currently available pages. */
   private final List<Integer> list;

   /**
    * Constructs a random replacement policy.
    *
    * @param numBuffers
    *           size of the buffer pool managed by this buffer policy
    */
   public RandomPolicy(final int numBuffers) {
      this.random = new Random();
      this.list = new ArrayList<>();
      for (int i = 0; i < numBuffers; i++) {
         this.list.add(i);
      }
   }

   @Override
   public void stateChanged(final int pos, final PageState newState) {
      switch(newState) {
         case FREE:
            // no need to update collection
            break;
         case PINNED:
            this.list.remove(Integer.valueOf(pos));
            break;
         case UNPINNED:
         default:
            this.list.add(pos);
            break;
      }
   }

   @Override
   public int pickVictim() {
      if (!this.list.isEmpty()) {
         final int victim = this.random.nextInt(this.list.size());
         return this.list.remove(victim);
      }
      return -1;
   }
}

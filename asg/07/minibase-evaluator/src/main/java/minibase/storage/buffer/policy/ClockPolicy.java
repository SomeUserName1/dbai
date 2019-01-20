/*
 * @(#)ClockPolicy.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer.policy;

import java.util.Arrays;


/**
 * A replacement policy that implements the <em>clock</em> algorithm. The clock policy is
 * similar to the LRU policy but has less overhead.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public class ClockPolicy implements ReplacementPolicy {
   /** The states of the pages managed by this buffer policy. */
   private final PageState[] pageStates;

   /** Clock head required for the default clock algorithm. */
   private int head;

   /**
    * Constructs a clock replacement policy.
    *
    * @param numBuffers
    *           size of the buffer pool managed by this buffer policy
    */
   public ClockPolicy(final int numBuffers) {
      // initialize the frame states
      this.pageStates = new PageState[numBuffers];
      Arrays.fill(this.pageStates, PageState.FREE);
      // initialize the clock head
      this.head = 0;
   }

   @Override
   public void stateChanged(final int pos, final PageState newState) {
      this.pageStates[pos] = newState;
   }

   @Override
   public int pickVictim() {
      // keep track of the number of tries
      final int size = this.pageStates.length;
      for (int i = 0; i < 2 * size; i++) {
         // update the clock head
         this.head = (this.head + 1) % size;
         switch (this.pageStates[this.head]) {
            case FREE:
               // return the victim page
               return this.head;
            case UNPINNED:
               // make referenced frames available next time
               this.pageStates[this.head] = PageState.FREE;
               break;
            default:
               // nothing to do
               break;
         }
      }
      // error state if the clock can not identify a victim
      return -1;
   }
}

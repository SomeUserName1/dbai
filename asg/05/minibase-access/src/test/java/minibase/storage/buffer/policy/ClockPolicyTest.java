/*
 * @(#)ClockPolicyTest.java   1.0   Nov 16, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer.policy;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import minibase.storage.buffer.policy.ReplacementPolicy.PageState;

/**
 * Test cases for {@link ClockPolicy}.
 *
 * @author Manfred Schaefer &lt;manfred.schaefer@uni.kn&gt;
 */
public class ClockPolicyTest {

   /** Size of the buffer pool on which the replacement policy works. */
   private static final int SIZE = 10000;

   /** Random number generator. */
   private static final Random RND = new Random(1337);

   /** Number of pages that should be unpined. */
   private static final int UNPIN = 500;

   /** Number of pages that should not be repined. */
   private static final int DONT_REPIN = 100;

   /** The policy implementation that is tested. */
   private ReplacementPolicy policy;

   /**
    * Initializes an LRU policy and pins all pages.
    */
   @Before
   public final void setUp() {
      // create policy
      this.policy = new ClockPolicy(SIZE);
      // pin all pages
      for (int pos = 0; pos < SIZE; ++pos) {
          this.policy.stateChanged(pos, PageState.PINNED);
      }
   }

   /**
    * Test method for {@link minibase.storage.buffer.ClockPolicy#pickVictim()}.
    */
   @Test
   public final void testPickSingleVictim() {

      // no victim to be found
      assertEquals(-1, this.policy.pickVictim());

      // unpin specific frame descriptor to be picked by policy
      final int rand = RND.nextInt(SIZE);
      this.policy.stateChanged(rand, PageState.UNPINNED);

      assertEquals(rand, this.policy.pickVictim());
   }


   /**
    * Test method for {@link minibase.storage.buffer.ClockPolicy#pickVictim()}.
    */
   @Test
   public final void testPickSequenceVictim() {
       // no victim to be found
       assertEquals(-1, this.policy.pickVictim());

       // unpin random pages
       final int victim;
       final ArrayList<Integer> randoms = new ArrayList<>();
       for (int i = 0; i < UNPIN; i++) {
           int rand;
           do {
               rand = RND.nextInt(SIZE);
           } while (randoms.contains(rand));
           randoms.add(rand);
       }

       for (final Integer rand : randoms) {
           this.policy.stateChanged(rand, PageState.UNPINNED);
       }

      // choose a victim and pin it; the victim should be one of the unpinned pages
       victim = this.policy.pickVictim();
       assertEquals(true, randoms.contains(victim));
       this.policy.stateChanged(victim, PageState.PINNED);

      // pin all but one unpinned pages
       for (int i  = 1; i < UNPIN - 1; i++) {
           this.policy.stateChanged(randoms.get((randoms.indexOf(victim) + i) % UNPIN), PageState.PINNED);
       }

      // unpin them again
       for (int i  = 0; i < UNPIN - 1; i++) {
           this.policy.stateChanged(randoms.get((randoms.indexOf(victim) + i) % UNPIN), PageState.UNPINNED);
       }

      // the one that was not pined should be picked
       assertEquals((int) randoms.get((randoms.indexOf(victim) + UNPIN - 1) % UNPIN), this.policy.pickVictim());

       final ArrayList<Integer> notRepined = new ArrayList<>();

      // pin all but don't repin unpinned pages
       for (int i  = 1; i < UNPIN; i++) {
           if (i < UNPIN - DONT_REPIN) {
               this.policy.stateChanged(randoms.get((randoms.indexOf(victim) + i) % UNPIN), PageState.PINNED);
           } else {
               notRepined.add(randoms.get((randoms.indexOf(victim) + i) % UNPIN));
           }
       }

      // unpin them again
       for (int i  = 0; i < UNPIN - DONT_REPIN; i++) {
           this.policy.stateChanged(randoms.get((randoms.indexOf(victim) + i) % UNPIN), PageState.UNPINNED);
       }


      // the one of those which were not pined should be picked
       assertEquals(true, notRepined.contains(this.policy.pickVictim()));
   }
}

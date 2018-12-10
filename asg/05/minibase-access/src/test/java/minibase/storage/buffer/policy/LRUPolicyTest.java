/*
 * @(#)LRUPolicyTest.java   1.0   Aug 2, 2006
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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import minibase.storage.buffer.policy.ReplacementPolicy.PageState;

/**
 * Test cases for {@link LRUPolicy}.
 *
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 */
public class LRUPolicyTest {

   /** Size of the buffer pool on which the replacement policy works. */
   private static final int SIZE = 100;

   /** Random number generator. */
   private static final Random RND = new Random(1337);

   /** Sequence of unpins. */
   private List<Integer> unpins;

   /** The policy implementation that is tested. */
   private ReplacementPolicy policy;

   /**
    * Initializes an LRU policy and pins all pages.
    */
   @Before
   public final void setUp() {
      this.policy = new LRUPolicy(SIZE);
      for (int pos = 0; pos < SIZE; ++pos) {
         this.policy.stateChanged(pos, PageState.PINNED);
      }
      this.unpins = new ArrayList<>(SIZE);
      for (int i = 0; i < SIZE; i++) {
         this.unpins.add(i);
      }
   }

   /**
    * Test method for {@link minibase.storage.buffer.LRUPolicy#pickVictim()}.
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
    * Test method for {@link minibase.storage.buffer.LRUPolicy#pickVictim()}.
    */
   @Test
   public final void testPickSequenceVictim() {

      // no victim to be found
      assertEquals(-1, this.policy.pickVictim());

      // unpin random sequence of frame descriptors
      Collections.shuffle(this.unpins);
      for (final Integer pos : this.unpins) {
         this.policy.stateChanged(pos, PageState.UNPINNED);
      }

      // first from sequence should be least-recently used, then next, etc.
      for (final Integer pos : this.unpins) {
         assertEquals((int) pos, this.policy.pickVictim());
         this.policy.stateChanged(pos, PageState.PINNED);
      }
   }
}

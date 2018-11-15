/*
 * @(#)ClockPolicy.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer.policy;

/**
 * A replacement policy that implements the <em>clock</em> algorithm. The clock policy
 * evicts the next unpinned memory page (buffer page) in the ring buffer that was not 
 * referenced since the last rotation.
 *
 * @author Simon Suckut, Fabian Klopfer
 * @version 1.0
 */
public class ClockPolicyGroup05 implements ReplacementPolicy {
   
   /** Table of referenced pages. */
   private byte[] referenced;
   /** Table of pinned pages. */
   private byte[] pinned;
   /** Pointer to the current clock position. */
   private int current;
   /** Total numer of buffer pages. */
   private final int numBuffers;

   /**
    * Constructs a Clock ReplacementPolicy.
    * 
    * @param numBuffers size of the buffer pool managed by this buffer policy
    */
   public ClockPolicyGroup05(final int numBuffers) {
      final int numBytes = (numBuffers + 7) >> 3;
      this.referenced = new byte[numBytes];
      this.pinned = new byte[numBytes];
      this.current = 0;
      this.numBuffers = numBuffers;
   }

   @Override
   public void stateChanged(final int pos, final PageState newState) {
      final int slot = pos >> 3;
      final byte mask = (byte) (0x1 << (pos & 0x7));
      switch (newState) {
         case FREE:
            // free pages do not get a second change
            this.referenced[slot] &= ~mask;
            break;
         case PINNED: 
            // mark page as pinned
            this.pinned[slot] |= mask;
            break;
         case UNPINNED:
            this.pinned[slot] &= ~mask;
            break;
         default:
            break;
      }
   }

   @Override
   public int pickVictim() {
      
      int rounds = 0;
      
      // stop after 2 rotations
      while (rounds < (2 * this.numBuffers)) {
         
         final int slot = this.current >> 3;
         final byte mask = (byte) (0x1 << (this.current & 0x7));
         
         // check if page is pinned
         if ((this.pinned[slot] & mask) == 0x0) {
            
            // return page if it is not referenced and dereference it
            if ((this.referenced[slot] & mask) == 0x0) {
               this.referenced[slot] &= ~mask;
               final int victim = this.current;
               this.current = (this.current + 1) % this.numBuffers;
               return victim;
            }
            this.referenced[slot] &= ~mask;
         }
         this.current = (this.current + 1) % this.numBuffers;
         rounds++;
      }
      return -1;
   }
}

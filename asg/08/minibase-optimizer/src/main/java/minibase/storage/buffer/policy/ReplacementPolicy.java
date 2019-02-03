/*
 * @(#)ReplacementPolicy.java   1.0   Oct 11, 2013
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
 * A replacement policy decides, which memory page (buffer page) is evicted from the buffer, if
 * memory needs to be freed up.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @version 1.0
 */
public interface ReplacementPolicy {
   /**
    * A buffer page (main-memory page) can be in one of three states. If a buffer page is pinned, its state
    * is {@link #PINNED}. Once the pin count of a buffer page drops to zero, the state of the
    * buffer page becomes {@link #UNPINNED}. Finally, if a buffer page is freed, its state changes to
    * {@link #FREE}.
    */
   enum PageState {
      /** Marks an available page. */
      FREE,

      /** Marks a referenced page (pin count of zero). */
      UNPINNED,

      /** Marks a pinned page (pin count larger than zero). */
      PINNED
   }

   /**
    * Notifies the replacement policy that the state of the page at the given position has changed.
    * @param pos position of the page in the buffer pool
    * @param newState new state of the page
    */
   void stateChanged(int pos, PageState newState);

   /**
    * Tries to pick a page to be evicted from the buffer pool.
    * @return the page's index if one could be identified, {@code -1} if all pages are pinned
    */
   int pickVictim();
}

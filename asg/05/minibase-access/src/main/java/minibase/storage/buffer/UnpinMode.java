/*
 * @(#)UnpinMode.java   1.0   Oct 30, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.buffer;

/**
 * The different modes for unpinning pages with the {@link BufferManager}.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public enum UnpinMode {
   /** There were no writes to the buffer, it does not have to be written back to disk. */
   CLEAN,
   /** The page was modified, it needs to be flushed to disk when it is replaced by the buffer manager. */
   DIRTY
}

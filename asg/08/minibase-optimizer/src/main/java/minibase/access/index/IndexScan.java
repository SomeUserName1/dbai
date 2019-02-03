/*
 * @(#)IndexScan.java   1.0   Oct 12, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.index;

import java.util.Iterator;

/**
 * Marker interface to combine behavior that needs to be implemented by scans over indexes in
 * Minibase.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public interface IndexScan extends Iterator<IndexEntry>, AutoCloseable {
   // marker interface

   @Override
   void close();
}

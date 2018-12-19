/*
 * @(#)IndexScan.java   1.0   Dec 02, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access;

import java.util.Iterator;

/**
 * Scan over the entries of an unclustered index.
 *
 * @author Manuel Hotz &lt;manuel.hotz&gt;
 */
public interface IndexScan extends Iterator<IndexEntry>, AutoCloseable {
   // marker interface

   @Override
   void close();
}

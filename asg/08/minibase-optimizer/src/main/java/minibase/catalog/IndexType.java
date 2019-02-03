/*
 * @(#)IndexType.java   1.0   Jun 22, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

/**
 * Defines the different index types in the Minibase catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public enum IndexType {

   /** Bitmap index. */
   BITMAP,
   /** Bitmap join index. */
   BITMAP_JOIN,
   /** Static hash index. */
   SHASH,
   /** Extendible hash index. */
   EHASH,
   /** Linear hash index. */
   LHASH,
   /** B-Tree index. */
   BTREE;
}

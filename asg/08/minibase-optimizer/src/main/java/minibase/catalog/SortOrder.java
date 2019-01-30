/*
 * @(#)SortOrder.java   1.0   Feb 20, 2014
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
 * Defines the possible sort orders of a sort key.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public enum SortOrder {

   /** Unsorted sort order. */
   UNSORTED,
   /** Ascending sort order. */
   ASCENDING,
   /** Descending sort order. */
   DESCENDING,
   /** Hashing "sort" order. */
   HASHED;
}

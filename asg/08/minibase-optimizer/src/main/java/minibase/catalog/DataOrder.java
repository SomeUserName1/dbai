/*
 * @(#)DataOrder.java   1.0   Feb 15, 2014
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
 * Enumeration that defines the different data orders for tables in the Minibase catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public enum DataOrder {

   /** Any other file type. */
   ANY,
   /** Heap file. */
   HEAP,
   /** Sorted file. */
   SORTED,
   /** Hashed file. */
   HASHED;
}

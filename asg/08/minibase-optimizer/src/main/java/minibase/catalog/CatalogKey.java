/*
 * @(#)CatalogKey.java   1.0   Sep 24, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import minibase.AbstractKey;

/**
 * A key is represented in the Minibase system catalog as a set of column identifiers.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class CatalogKey extends AbstractKey<Integer> {

   /**
    * Constructs a new column set with the given array of columns IDs.
    *
    * @param columns
    *           array of key column identifiers
    */
   public CatalogKey(final int... columns) {
      super(Arrays.stream(columns).mapToObj(i -> Integer.valueOf(i)).collect(Collectors.toList()));
   }

   @Override
   public boolean equals(final List< ? > columns, final List< ? > other) {
      return columns.containsAll(other);
   }
}

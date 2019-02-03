/*
 * @(#)CatalogForeignKey.java   1.0   Feb 15, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import minibase.AbstractForeignKey;

/**
 * A foreign key is represented in the Minibase system catalog as a key consisting the referencing column
 * identifiers and a key consisting of the referenced column identifiers.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class CatalogForeignKey extends AbstractForeignKey<CatalogKey> {

   /**
    * Constructs a new foreign key with the given key columns and the given referenced columns in the system
    * catalog.
    *
    * @param referencing
    *           referencing key
    * @param referenced
    *           referenced key
    */
   public CatalogForeignKey(final CatalogKey referencing, final CatalogKey referenced) {
      super(referencing, referenced);
   }
}

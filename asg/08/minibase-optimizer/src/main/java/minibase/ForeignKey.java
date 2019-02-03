/*
 * @(#)ForeignKey.java   1.0   Mar 3, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

/**
 * Common interface for foreign keys in Minibase.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @param <T>
 *           generic type of the columns in this foreign key
 */
public interface ForeignKey<T extends Key< ? >> {

   /**
    * Returns the referencing columns of this foreign key.
    *
    * @return referencing columns
    */
   T getReferencingColumns();

   /**
    * Returns the referenced columns of this foreign key.
    *
    * @return referenced columns
    */
   T getReferencedColumns();
}

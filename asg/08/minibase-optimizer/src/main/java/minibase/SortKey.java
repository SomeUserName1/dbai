/*
 * @(#)SortKey.java   1.0   Mar 5, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.util.List;

import minibase.catalog.SortOrder;
import minibase.query.schema.ColumnReference;

/**
 * Common interface for keys in Minibase.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @param <T>
 *           generic type of the columns in this key
 */
public interface SortKey<T> extends Key<T> {

   /**
    * Returns the sort order of the column with the given index.
    *
    * @param index
    *           sort key column index
    * @return order of key column at given index
    */
   SortOrder getOrder(int index);

   /**
    * Checks whether this key is a match to the given list of columns. In order to be a match, the key columns
    * have to be in the same order as the given columns.
    *
    * @param columnReferences
    *           list of column references
    * @return {@code true} if this key is a match to the given list of columns, {@code false} otherwise
    */
   boolean isMatch(List<ColumnReference> columnReferences);
}

/*
 * @(#)Key.java   1.0   Mar 3, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Common interface for keys in Minibase.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @param <T>
 *           generic type of the columns in this key
 */
public interface Key<T> extends Iterable<T> {

   /**
    * Checks whether the given column is contained in this key.
    *
    * @param column
    *           column
    * @return {@code true} if the given column is contained in this key, {@code false} otherwise
    */
   boolean contains(T column);

   /**
    * Returns the column with the given index.
    *
    * @param index
    *           sort key column index
    * @return column at given index
    */
   T getColumn(int index);

   /**
    * Checks whether this key is a subset of the given collection of columns.
    *
    * @param columns
    *           column collection
    * @return {@code true} if this key is a subset of the given collection of columns, {@code false} otherwise
    */
   boolean isSubsetOf(Collection<T> columns);

   /**
    * Returns the number of columns of this key.
    *
    * @return number of columns
    */
   int size();

   /**
    * Opens a stream that returns the columns of this key.
    * 
    * @return column stream
    */
   Stream<T> stream();
}

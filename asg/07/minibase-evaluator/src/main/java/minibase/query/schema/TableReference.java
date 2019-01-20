/*
 * @(#)TableReference.java   1.0   Aug 23, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.List;
import java.util.Optional;

/**
 * Common interface of table references.
 * 
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public interface TableReference extends Reference {

   /**
    * Returns the column with the given name of this table.
    *
    * @param name
    *           column name
    * @return column representation
    */
   Optional<ColumnReference> getColumn(String name);

   /**
    * Returns all columns of this table.
    *
    * @return list of column representation
    */
   List<ColumnReference> getColumns();
}

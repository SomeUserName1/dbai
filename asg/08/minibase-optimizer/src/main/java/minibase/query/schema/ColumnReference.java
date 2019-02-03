/*
 * @(#)ColumnReference.java   1.0   Feb 26, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.Optional;

import minibase.catalog.DataType;

/**
 * Common interface of column references.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public interface ColumnReference extends Reference {

   /**
    * Returns the declared data type of this column reference.
    *
    * @return data type
    */
   DataType getType();

   /**
    * Returns the reference to the parent table of this column reference.
    *
    * @return parent reference or {@code null}, if this column reference has no parent table
    */
   Optional<TableReference> getParent();
}

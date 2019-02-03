/*
 * @(#)DerivedColumnReference.java   1.0   Jun 13, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.List;

/**
 * A derived column reference, i.e., a reference to a column that is computed from a set of input columns.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public interface DerivedColumnReference extends ColumnReference {

   /**
    * Returns the input columns of this derived column reference.
    *
    * @return list of input columns
    */
   List<ColumnReference> getInputColumns();

   /**
    * Returns whether the column is derived by aggregation.
    * 
    * @return {@code true} if this is an aggregated column, {@code false} otherwise
    */
   boolean isAggregation();
}

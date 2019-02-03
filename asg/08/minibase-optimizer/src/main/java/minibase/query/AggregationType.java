/*
 * @(#)AggregationType.java   1.0   Jun 12, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query;

import java.util.Optional;

import minibase.catalog.DataType;

/**
 * Enumeration describing the different types of aggregation functions supported by Minibase.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public enum AggregationType {

   /** AVG aggregation type. */
   AVG,
   /** SUM aggregation type. */
   SUM,
   /** MIN aggregation type. */
   MIN,
   /** MAX aggregation type. */
   MAX,
   /** COUNT aggregation type. */
   COUNT;

   /**
    * Returns the output type of this type of aggregation function based on the given input types.
    *
    * @param inputTypes
    *           input types
    * @return output type
    */
   public DataType getOutputType(final DataType... inputTypes) {
      switch (this) {
         case COUNT:
            return DataType.BIGINT;
         case AVG:
         case SUM:
         case MIN:
         case MAX:
            final Optional<DataType> result = DataType.getCommonType(inputTypes);
            if (result.isPresent()) {
               return result.get();
            }
         default:
            throw new IllegalStateException(
                  "Undefined output type for aggregation function " + this.name() + ".");
      }
   }
}

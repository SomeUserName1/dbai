/*
 * @(#)IndexStatistics.java   1.0   Jun 20, 2014
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
 * Minibase collects statistics for each index in the system catalog. These statistics include the cardinality
 * and unique cardinality of the values in the index.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public class IndexStatistics extends Statistics {

   /**
    * Creates a new set of statistical values that describe the data in an index.
    *
    * @param cardinality
    *           index cardinality
    * @param uniqueCardinality
    *           index unique cardinality
    */
   public IndexStatistics(final long cardinality, final long uniqueCardinality) {
      super(cardinality, uniqueCardinality);
   }

   /**
    * Creates a new set of statistical values that describe the data in an index.
    *
    * @param cardinality
    *           index cardinality
    * @param uniqueCardinality
    *           index unique cardinality
    * @deprecated Use {@link IndexStatistics#IndexStatistics(long, long)} instead.
    */
   @Deprecated
   public IndexStatistics(final double cardinality, final double uniqueCardinality) {
      super((long) cardinality, (long) uniqueCardinality);
   }

   /** Empty table statistics with all values initialized to zero. */
   private static final IndexStatistics EMPTY_STATS = new IndexStatistics(0, 0);

   /**
    * Returns empty index statistics with all values initialized to zero.
    *
    * @return empty index statistics
    */
   public static final IndexStatistics emptyStatistics() {
      return EMPTY_STATS;
   }
}

/*
 * @(#)TableStatistics.java   1.0   Feb 14, 2014
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
 * Minibase collects statistics for each table in the system catalog. These statistics include the cardinality
 * and unique cardinality of the values in the table. Additionally, the size of the table in blocks and the
 * width of its tuples are recorded.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public class TableStatistics extends Statistics {

   /** Average width of tuples in this table. */
   private final double width;

   /**
    * Creates unknown statistical values that describe the data in a table.
    */
   private TableStatistics() {
      this(UNKNOWN, UNKNOWN, UNKNOWN);
   }

   /**
    * Creates a new set of statistical values that describe the data in a table.
    *
    * @param cardinality
    *           cardinality of the table
    * @param uniqueCardinality
    *           unique cardinality of the table
    * @param width
    *           average tuple width
    */
   public TableStatistics(final double cardinality, final double uniqueCardinality, final double width) {
      super(cardinality, uniqueCardinality);
      this.width = width;
   }

   /**
    * Returns the tuple width of this table. If the table contains fixed-length columns only, the width
    * corresponds to the sum of the length of the column data types divided by the page size. If the table
    * contains variable-length columns, the width is a statistical value that captures the average length of
    * the stored tuples.
    *
    * @return average value width as a fraction of the page size
    */
   public double getWidth() {
      return this.width;
   }

   /** Empty table statistics with all values initialized to zero. */
   private static final TableStatistics EMPTY_STATS = new TableStatistics();

   /**
    * Returns empty table statistics with all values initialized to zero.
    *
    * @return empty table statistics
    */
   public static final TableStatistics emptyStatistics() {
      return EMPTY_STATS;
   }
}

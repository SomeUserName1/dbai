/*
 * @(#)ColumnStatistics.java   1.0   Feb 14, 2014
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
 * Minibase collects statistics for each table column in the system catalog. These statistics include the
 * cardinality and unique cardinality of the values in the column as well as their width. If applicable, the
 * Minibase system catalog also records the minimum and maximum value of a column. Finally, the system catalog
 * may also contain a histogram and/or a most common value distribution.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class ColumnStatistics extends Statistics {

   /** Minimum value of the column. */
   private final Object minimum;

   /** Maximum value of the column. */
   private final Object maximum;

   /** Average width of tuples in this column. */
   private final double width;

   /**
    * Creates a unknown statistical values that describe the data in a column.
    */
   private ColumnStatistics() {
      this(UNKNOWN, UNKNOWN, null, null, UNKNOWN);
   }

   /**
    * Creates a new set of statistical values that describe the data in a column. Values are initialized with
    * the given values.
    *
    * @param cardinality
    *           column cardinality
    * @param uniqueCardinality
    *           column unique cardinality
    * @param minimum
    *           minimum column value
    * @param maximum
    *           maximum column value
    * @param width
    *           average tuple width
    */
   public ColumnStatistics(final double cardinality, final double uniqueCardinality, final Object minimum,
         final Object maximum, final double width) {
      super(cardinality, uniqueCardinality);
      this.minimum = minimum;
      this.maximum = maximum;
      this.width = width;
   }

   /**
    * Returns the minimum value of the column.
    *
    * @return minimum column value
    */
   public Object getMinimum() {
      return this.minimum;
   }

   /**
    * Returns the maximum value of the column.
    *
    * @return maximum column value
    */
   public Object getMaximum() {
      return this.maximum;
   }

   /**
    * Returns the width of the values of the column. If the column consists of fixed-length values, the width
    * corresponds to the length of the column data type divided by the page size. If the column consists of
    * variable-length values, the width is a statistical value that it captures the average length of the
    * stored values.
    *
    * @return average value width as a fraction of the page size
    */
   public double getWidth() {
      return this.width;
   }

   /** Empty column statistics with all values initialized to zero or null. */

   private static final ColumnStatistics EMPTY_STATS = new ColumnStatistics();

   /**
    * Returns empty column statistics with all values initialized to zero or null.
    *
    * @return empty column statistics
    */
   public static final ColumnStatistics emptyStatistics() {
      return EMPTY_STATS;
   }
}

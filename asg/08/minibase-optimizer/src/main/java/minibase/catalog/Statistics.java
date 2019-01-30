/*
 * @(#)Statistics.java   1.0   Feb 14, 2014
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
 * Minibase collects statistics for the concepts described by the system catalog (tables, columns, indexes,
 * etc.). This class is a common abstract base class for these statistics objects.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public class Statistics {

   /** Unknown statistical value. */
   public static final double UNKNOWN = Double.NaN;

   /** Cardinality. */
   private final double cardinality;

   /** Unique cardinality. */
   private final double uniqueCardinality;

   /**
    * Creates a new set of statistical values.
    *
    * @param cardinality
    *           cardinality
    * @param uniqueCardinality
    *           unique cardinality
    */
   public Statistics(final double cardinality, final double uniqueCardinality) {
      this.cardinality = cardinality;
      this.uniqueCardinality = uniqueCardinality;
   }

   /**
    * Creates a new set of statistical values.
    *
    * @param cardinality
    *           cardinality
    */
   public Statistics(final double cardinality) {
      this(cardinality, cardinality);
   }

   /**
    * Creates a new set of statistical values. Values are initialized to zero as a default.
    */
   protected Statistics() {
      this(0);
   }

   /**
    * Returns the cardinality.
    *
    * @return cardinality
    */
   public double getCardinality() {
      return this.cardinality;
   }

   /**
    * Returns the unique cardinality, i.e., the number of unique values.
    *
    * @return unique cardinality
    */
   public double getUniqueCardinality() {
      return this.uniqueCardinality;
   }
}

/*
 * @(#)RecordComparator.java   1.0   Nov 19, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator.compare;

/**
 * This interface describes a comparator that can compare records located at given offsets in two byte-arrays
 * (or in the same byte-array).
 *
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 * @version 1.0
 */
public interface RecordComparator {

   /**
    * Compare records.
    *
    * @param array1
    *           byte-array containing first record
    * @param offset1
    *           offset to the first record
    * @param array2
    *           byte-array containing second record
    * @param offset2
    *           offset to the second record
    * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or
    *         greater than the second.
    */
   int compare(byte[] array1, int offset1, byte[] array2, int offset2);

   /**
    * Compares two records starting at index {@code 0} of their respective arrays.
    *
    * @param record1 first record
    * @param record2 second record
    * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or
    *         greater than the second.
    */
   default int compare(final byte[] record1, final byte[] record2) {
      return this.compare(record1, 0, record2, 0);
   }

   /**
    * Checks if the first record is less than the second record according to this comparator.
    *
    * @param record1 first record
    * @param record2 second record
    * @return {@code true} if the first record is less than the second, {@code false} otherwise
    */
   default boolean lessThan(final byte[] record1, final byte[] record2) {
      return this.compare(record1, record2) < 0;
   }

   /**
    * Checks if the first record is less than the second record according to this comparator.
    *
    * @param record1 array containing the first record
    * @param offset1 offset of the first record
    * @param record2 array containing the second record
    * @param offset2 offset of the second record
    * @return {@code true} if the first record is less than the second, {@code false} otherwise
    */
   default boolean lessThan(final byte[] record1, final int offset1, final byte[] record2, final int offset2) {
      return this.compare(record1, offset1, record2, offset2) < 0;
   }

   /**
    * Checks if the first record is less than or equal to the second record according to this comparator.
    *
    * @param record1 first record
    * @param record2 second record
    * @return {@code true} if the first record is less than or equal to the second, {@code false} otherwise
    */
   default boolean lessThanOrEqual(final byte[] record1, final byte[] record2) {
      return this.compare(record1, record2) <= 0;
   }

   /**
    * Checks if the first record is equal to the second record according to this comparator.
    *
    * @param record1 first record
    * @param record2 second record
    * @return {@code true} if the first record is equal to the second, {@code false} otherwise
    */
   default boolean equals(final byte[] record1, final byte[] record2) {
      return this.compare(record1, record2) == 0;
   }

   /**
    * Checks if the first record is greater than or equal to the second record according to this comparator.
    *
    * @param record1 first record
    * @param record2 second record
    * @return {@code true} if the first record is greater than or equal to the second, {@code false} otherwise
    */
   default boolean greaterThanOrEqual(final byte[] record1, final byte[] record2) {
      return this.compare(record1, record2) >= 0;
   }

   /**
    * Checks if the first record is greater than the second record according to this comparator.
    *
    * @param record1 first record
    * @param record2 second record
    * @return {@code true} if the first record is greater than the second, {@code false} otherwise
    */
   default boolean greaterThan(final byte[] record1, final byte[] record2) {
      return this.compare(record1, record2) > 0;
   }
}

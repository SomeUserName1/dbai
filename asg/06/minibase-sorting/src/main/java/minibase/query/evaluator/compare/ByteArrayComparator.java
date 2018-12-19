/*
 * @(#)ByteArrayComparator.java   1.0   Nov 4, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator.compare;

/**
 * This byte array comparator compares records as a whole, fields are not considered.
 * This implementation is used to eliminate duplicates with the external sort.
 * 
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 */
public class ByteArrayComparator implements RecordComparator {

   /** Record length. */
   private final int recordLength;

   /**
    * Instantiate byte array comparator.
    * 
    * @param recordLength record length
    */
   public ByteArrayComparator(final int recordLength) {
      this.recordLength = recordLength;
   }

   @Override
   public int compare(final byte[] array1, final int offset1, final byte[] array2, final int offset2) {
      for (int i = 0; i < this.recordLength; i++) {
         final int comp = Byte.compare(array1[offset1 + i], array2[offset2 + i]);
         if (comp != 0) {
            return comp;
         }
      }
      return 0;
   }
}

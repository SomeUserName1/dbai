/*
 * @(#)TupleComparator.java   1.0   Apr 11, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator.compare;

import java.util.Arrays;

import minibase.catalog.DataType;
import minibase.query.schema.Schema;

/**
 * Compares two records, potentially from different relations.
 *
 * @author Michael Delz &lt;michael.delz@uni-konstanz.de&gt;
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 */
public class TupleComparator implements RecordComparator {

   /** Types of comparisons. */
   private enum ComparisonType {
      /** Values are compared as 64-bit integers. */
      INTEGRAL,
      /** Values are compared as 64-bit floating-point numbers. */
      FLOATING,
      /** Values are compared as strings. */
      STRING,
      /** Values are compared as date-time values. */
      DATE_TIME,
      /** Values are compared as times. */
      TIME
   }

   /** Schema of the left tuple. */
   private final Schema schema1;

   /** Field numbers from the first schema. */
   private final int[] fieldNumbers1;

   /** Schema if the right tuple. */
   private final Schema schema2;

   /** Field numbers from sche second schema. */
   private final int[] fieldNumbers2;

   /** Types of comparison for each field pair. */
   private final ComparisonType[] comparisonTypes;

   /** Flags for sorting order. */
   private final boolean[] ascending;

   /**
    * Compares two tuple of the same relation with the given schema on the given fields. All sort orders are
    * ascending.
    *
    * @param schema
    *           tuple schema
    * @param fields
    *           fields to compare
    */
   public TupleComparator(final Schema schema, final int... fields) {
      this(schema, fields, trues(fields.length));
   }

   /**
    * Creates a comparator that compares two tuples of the same relation.
    *
    * @param schema
    *           schema of the tuples
    * @param fields
    *           fields to compare
    * @param sortOrders
    *           sorting orders
    */
   public TupleComparator(final Schema schema, final int[] fields, final boolean[] sortOrders) {
      this(schema, fields, schema, fields, sortOrders);
   }

   /**
    * Creates a comparator that compares two tuples from the given schemas according to the given fields. All
    * sort orders are ascending.
    *
    * @param schema1
    *           first tuple's schema
    * @param fieldNumbers1
    *           first tuple's fields to compare
    * @param schema2
    *           second tuple's schema
    * @param fieldNumbers2
    *           second tuple's fields to compare
    */
   public TupleComparator(final Schema schema1, final int[] fieldNumbers1, final Schema schema2,
         final int[] fieldNumbers2) {
      this(schema1, fieldNumbers1, schema2, fieldNumbers2, trues(fieldNumbers1.length));
   }

   /**
    * New Instance.
    *
    * @param schema1
    *           schema one
    * @param fieldNumbers1
    *           fields from schema one
    * @param schema2
    *           schema two
    * @param fieldNumbers2
    *           fields from schema two
    * @param ascending
    *           flags for ascending order
    */
   public TupleComparator(final Schema schema1, final int[] fieldNumbers1, final Schema schema2,
         final int[] fieldNumbers2, final boolean[] ascending) {
      if (fieldNumbers1.length != fieldNumbers2.length) {
         throw new IllegalStateException("Compared fields do not have the same cardinality.");
      }
      final int n = fieldNumbers1.length;

      this.schema1 = schema1;
      this.fieldNumbers1 = fieldNumbers1;
      this.schema2 = schema2;
      this.fieldNumbers2 = fieldNumbers2;
      this.comparisonTypes = new ComparisonType[n];
      this.ascending = ascending;

      for (int i = 0; i < n; i++) {
         final DataType left = schema1.getColumn(this.fieldNumbers1[i]).getType();
         final DataType right = schema2.getColumn(this.fieldNumbers2[i]).getType();
         if (left.isNumeric() && right.isNumeric()) {
            this.comparisonTypes[i] = left.isFloatingPoint() || right.isFloatingPoint()
                  ? ComparisonType.FLOATING : ComparisonType.INTEGRAL;
         } else if (left.isString() && right.isString()) {
            this.comparisonTypes[i] = ComparisonType.STRING;
         } else if (left.isTemporal() && right.isTemporal()) {
            if ((left == DataType.DATE || left == DataType.DATETIME)
                  && (right == DataType.DATE || right == DataType.DATETIME)) {
               this.comparisonTypes[i] = ComparisonType.DATE_TIME;
            } else if (left == DataType.TIME && right == DataType.TIME) {
               this.comparisonTypes[i] = ComparisonType.TIME;
            }
         }

         if (this.comparisonTypes[i] == null) {
            throw new IllegalStateException("Fields at position " + (i + 1) + " cannot be compared.\n"
                  + "\tLeft(field:" + this.fieldNumbers1[i] + ", type:" + left + ")\n" + "\tRight(field:"
                  + this.fieldNumbers2[i] + ", type:" + right + ")");
         }
      }
   }

   /**
    * Returning a boolean array containing only {@code true} values.
    *
    * @param length
    *           length of the array
    * @return the array
    */
   private static boolean[] trues(final int length) {
      final boolean[] values = new boolean[length];
      Arrays.fill(values, true);
      return values;
   }

   @Override
   public int compare(final byte[] array1, final int offset1, final byte[] array2, final int offset2) {
      for (int i = 0; i < this.fieldNumbers1.length; i++) {
         final int a = this.fieldNumbers1[i];
         final int b = this.fieldNumbers2[i];
         final int cmp;
         switch (this.comparisonTypes[i]) {
            case INTEGRAL:
               cmp = Long.compare(this.schema1.getIntegral(array1, offset1, a),
                     this.schema2.getIntegral(array2, offset2, b));
               break;
            case FLOATING:
               cmp = Double.compare(this.schema1.getFloating(array1, offset1, a),
                     this.schema2.getFloating(array2, offset2, b));
               break;
            case STRING:
               cmp = this.schema1.getString(array1, offset1, a)
                     .compareTo(this.schema2.getString(array2, offset2, b));
               break;
            case DATE_TIME:
               cmp = this.schema1.getDateTime(array1, offset1, a)
                     .compareTo(this.schema2.getDateTime(array2, offset2, b));
               break;
            case TIME:
            default:
               cmp = this.schema1.getTime(array1, offset1, a)
                     .compareTo(this.schema2.getTime(array2, offset2, b));
               break;
         }
         if (cmp != 0) {
            return this.ascending[i] ? cmp : -cmp;
         }
      }
      return 0;
   }
}

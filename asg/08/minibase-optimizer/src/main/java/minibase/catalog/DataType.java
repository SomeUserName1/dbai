/*
 * @(#)DataType.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.function.Function;

/**
 * Enumeration that defines the attribute types supported by the Minibase catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public enum DataType {

   /** BIGINT attribute type (8 bytes, fixed-length). */
   BIGINT(true, 8, true, Long::valueOf, new int[] { 0, 0, 0, 0, 4, 4, -1, -1, -1, -1, -1 }),
   /** INT attribute type (4 bytes, fixed-length). */
   INT(true, 4, true, Integer::valueOf, new int[] { 0, 1, 1, 1, 4, 5, -1, -1, -1, -1, -1 }),
   /** SMALLINT attribute type (2 bytes, fixed-length). */
   SMALLINT(true, 2, true, Short::valueOf, new int[] { 0, 1, 2, 2, 4, 5, -1, -1, -1, -1, -1 }),
   /** TINYINT attribute type (1 byte, fixed-length). */
   TINYINT(true, 1, true, Byte::valueOf, new int[] { 0, 1, 2, 3, 4, 5, -1, -1, -1, -1, -1 }),
   /** DOUBLE attribute type (8 bytes, fixed-length). */
   DOUBLE(true, 8, true, Double::valueOf, new int[] { 4, 4, 4, 4, 4, 4, -1, -1, -1, -1, -1 }),
   /** FLOAT attribute type (4 bytes, fixed-length). */
   FLOAT(true, 4, true, Float::valueOf, new int[] { 4, 5, 5, 5, 4, 5, -1, -1, -1, -1, -1 }),
   /** DATE attribute type (3 bytes for day, month, and year each, fixed-length). */
   DATE(true, 3, false, Date::valueOf, new int[] { -1, -1, -1, -1, -1, -1, 6, -1, -1, -1, -1 }),
   /** TIME attribute type (5 bytes, fixed-length). */
   TIME(true, 5, false, Time::valueOf, new int[] { -1, -1, -1, -1, -1, -1, -1, 7, -1, -1, -1 }),
   /** DATETIME attribute type (8 bytes, combines DATE and TIME, fixed-length). */
   DATETIME(true, 8, false, Timestamp::valueOf, new int[] { -1, -1, -1, -1, -1, -1, -1, -1, 8, -1, -1 }),
   /** CHAR attribute type (user-defined number of bytes, fixed-length). */
   CHAR(true, -1, false, s -> s, new int[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, 9, -1 }),
   /** VARCHAR attribute type (user-defined maximum number of bytes, variable-length. */
   VARCHAR(false, -1, false, s -> s, new int[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10 });

   /** Records whether this attribute type has a fixed-length values or not. */
   private final boolean fixedLength;

   /** Size of the values for this attribute type. */
   private final int size;

   /** Records whether this type is numeric or not. */
   private final boolean numeric;

   /** Function that creates data values of this type from a string representation. */
   private final Function<String, ? > function;

   /** Compatibility matrix of this type. */
   private final int[] compatibility;

   /**
    * Constructs a new attribute type.
    *
    * @param fixedLength
    *           {@code true} if values of this type have a fixed length, {@code false} otherwise.
    * @param size
    *           size of the values of this type in bytes, or -1 if they are of variable or used-defined size
    * @param numeric
    *           if the type is numeric
    * @param fromStringFunction
    *           function that creates data value of this type from a string representation
    * @param compatibility
    *           compatibility matrix of this type
    * @param <T>
    *           Java type that corresponds to this data type
    */
   <T> DataType(final boolean fixedLength, final int size, final boolean numeric,
         final Function<String, T> fromStringFunction, final int[] compatibility) {
      this.fixedLength = fixedLength;
      this.size = size;
      this.numeric = numeric;
      this.function = fromStringFunction;
      this.compatibility = compatibility;
   }

   /**
    * Returns {@code true} if values of this attribute types are of a fixed length and {@code false}
    * otherwise.
    *
    * @return {@code true} if this attribute type has fixed-length values, {@code false} otherwise
    */
   public boolean isFixedLength() {
      return this.fixedLength;
   }

   /**
    * Returns {@code true} if this type is numeric and {@code false} otherwise.
    *
    * @return {@code true} if this type is numeric and {@code false} otherwise
    */
   public boolean isNumeric() {
      return this.numeric;
   }

   /**
    * Checks if this data type is a whole number.
    *
    * @return {@code true} if the data type is integral, {@code false} otherwise
    */
   public boolean isIntegral() {
      switch (this) {
         case TINYINT:
         case SMALLINT:
         case INT:
         case BIGINT:
            return true;
         default:
            return false;
      }
   }

   /**
    * Checks if this data type is a floating-point number.
    *
    * @return {@code true} if the data type is a floating-point number, {@code false} otherwise
    */
   public boolean isFloatingPoint() {
      switch (this) {
         case FLOAT:
         case DOUBLE:
            return true;
         default:
            return false;
      }
   }

   /**
    * Checks if this data type stores time-related data.
    *
    * @return {@code true} if the data type is temporal, {@code false} otherwise
    */
   public boolean isTemporal() {
      switch (this) {
         case TIME:
         case DATE:
         case DATETIME:
            return true;
         default:
            return false;
      }
   }

   /**
    * Returns {@code true} if this type is a String (CHAR, VARCHAR) and {@code false} otherwise.
    *
    * @return {@code true} if this type is a String (CHAR, VARCHAR) and {@code false} otherwise
    */
   public boolean isString() {
      switch (this) {
         case VARCHAR:
         case CHAR:
            return true;
         default:
            return false;
      }
   }

   /**
    * Returns the declared size of the values of this attribute type in bytes or {@code -1} if the length of
    * the values is variable or defined by the user.
    *
    * @return length of the values of this attribute type, {@code -1} indicates values of variable or
    *         user-defined length.
    */
   public int getSize() {
      return this.size;
   }

   /**
    * Returns the common type of this type and the given other type based on the compatibility matrix.
    *
    * @param other
    *           other data type
    * @return common data type
    */
   public Optional<DataType> getCommonType(final DataType other) {
      final int ordinal = this.compatibility[other.ordinal()];
      if (ordinal >= 0 && ordinal < values().length) {
         return Optional.of(values()[ordinal]);
      }
      return Optional.empty();
   }

   /**
    * Parses a string representation and returns a data value of this type.
    *
    * @param fromString
    *           string representation of the value
    * @param <T>
    *           Java type that corresponds to this data type
    * @return Java representation of the value
    */
   @SuppressWarnings("unchecked")
   public <T> T parse(final String fromString) {
      return (T) this.function.apply(fromString);
   }

   /**
    * Returns the common type of this type and the given other types based on the compatibility matrix.
    *
    * @param types
    *           other data types
    * @return common data type
    */
   public static Optional<DataType> getCommonType(final DataType[] types) {
      if (types == null || types.length == 0) {
         throw new IllegalArgumentException("Type list cannot be null or empty.");
      }
      DataType result = types[0];
      for (int i = 1; i < types.length; i++) {
         final DataType type = types[i];
         final Optional<DataType> common = result.getCommonType(type);
         if (common.isPresent()) {
            result = common.get();
         } else {
            // Undefined common type
            return Optional.empty();
         }
      }
      return Optional.of(result);
   }
}

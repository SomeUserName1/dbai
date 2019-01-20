/*
 * @(#)SchemaKey.java   1.0   Mar 4, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.ArrayList;
import java.util.List;

import minibase.AbstractKey;

/**
 * In the schema for query expressions, a key is represented as a set of column references.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class SchemaKey extends AbstractKey<ColumnReference> {

   /**
    * Constructs a new schema key with the given set of column references.
    *
    * @param columns
    *           column reference set
    */
   public SchemaKey(final List<ColumnReference> columns) {
      super(columns);
   }

   @Override
   public boolean equals(final List< ? > columns, final List< ? > other) {
      return columns.containsAll(other);
   }

   /**
    * Checks whether this key is a subset of the given schema.
    *
    * @param schema
    *           schema
    * @return {@code true} if this key is a subset of the given collection of columns, {@code false} otherwise
    */
   public boolean isSubsetOf(final Schema schema) {
      if (schema.getColumnCount() < this.size()) {
         return false;
      }
      for (final ColumnReference column : this) {
         if (!schema.containsColumn(column)) {
            return false;
         }
      }
      return true;
   }

   /**
    * Returns the union of this schema key and the given other schema key.
    *
    * @param other
    *           schema key
    * @return union of this and the other key
    */
   public SchemaKey union(final SchemaKey other) {
      final List<ColumnReference> columns = new ArrayList<>();
      for (final ColumnReference column : this) {
         columns.add(column);
      }
      for (final ColumnReference column : other) {
         if (!columns.contains(column)) {
            columns.add(column);
         }
      }
      return new SchemaKey(columns);
   }
}

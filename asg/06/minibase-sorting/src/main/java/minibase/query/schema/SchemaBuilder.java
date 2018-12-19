/*
 * @(#)SchemaBuilder.java   1.0   Jun 13, 2016
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import minibase.catalog.DataType;

/**
 * This class is used to construct {@link Schema} instances for the evaluator.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public class SchemaBuilder {

   /** Columns in the schema. */
   private final List<ColumnReference> columns = new ArrayList<>();

   /**
    * Creates a new, empty schema builder.
    */
   public SchemaBuilder() {
   }

   /**
    * Creates a schema builder initialized with the given schema.
    *
    * @param original
    *           initial state of this builder
    */
   public SchemaBuilder(final Schema original) {
      this.addAll(original);
   }

   /**
    * Creates a schema builder initialized with selected columns from the given schema.
    *
    * @param original
    *           schema to take columns from
    * @param columns
    *           columns of {@code original} to copy
    */
   public SchemaBuilder(final Schema original, final int[] columns) {
      for (final int col : columns) {
         this.columns.add(original.getColumn(col));
      }
   }

   /**
    * Adds a column with the given type, length, and name.
    *
    * @param name
    *           the name of the field
    * @param type
    *           the type of the field
    * @param length
    *           the length of the field in bytes
    * @return self-reference for convenience
    */
   public SchemaBuilder addField(final String name, final DataType type, final int length) {
      this.columns.add(new SchemaColumn(name, type, length));
      return this;
   }

   /**
    * Adds the column with the given number from the given schema to this builder.
    *
    * @param schema
    *           schema to copy the column from
    * @param index
    *           column index
    * @return self-reference for convenience
    */
   public SchemaBuilder addField(final Schema schema, final int index) {
      this.columns.add(schema.getColumn(index));
      return this;
   }

   /**
    * Adds all columns of the given schema to this builder.
    *
    * @param other
    *           schema to add
    * @return self-reference for convenience
    */
   public SchemaBuilder addAll(final Schema other) {
      for (final ColumnReference column : other) {
         this.columns.add(column);
      }
      return this;
   }

   /**
    * Creates the finished schema.
    *
    * @return the schema
    */
   public Schema build() {
      return new Schema(this.columns, Collections.emptyList(), Collections.emptySet(),
            Collections.emptySet());
   }

   /**
    * Private class implementing the {@link ColumnReference} interface to manage the columns of the schema.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
    * @since 1.0
    */
   private final class SchemaColumn implements ColumnReference {

      /** Column name. */
      private final String name;
      /** Column type. */
      private final DataType type;
      /** Column size. */
      private final int size;

      /**
       * Creates a new column with the given name, type, and size.
       *
       * @param name
       *           column name
       * @param type
       *           column type
       * @param size
       *           column size
       */
      private SchemaColumn(final String name, final DataType type, final int size) {
         this.name = name;
         this.type = type;
         this.size = size;
      }

      @Override
      public int getID() {
         throw new UnsupportedOperationException("Method SchemaColumn#getID() is not supported.");
      }

      @Override
      public String getName() {
         return this.name;
      }

      @Override
      public int getSize() {
         return this.size;
      }

      @Override
      public double getWidth() {
         throw new UnsupportedOperationException("Method SchemaColumn#getWidth() is not supported.");
      }

      @Override
      public DataType getType() {
         return this.type;
      }

      @Override
      public Optional<TableReference> getParent() {
         throw new UnsupportedOperationException("Method SchemaColumn#getParent() is not supported.");
      }

      @Override
      public String toString() {
         return this.name;
      }
   }
}

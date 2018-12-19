/*
 * @(#)ColRef.java   1.0   Jun 17, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator.predicate;

import java.util.Optional;

import minibase.catalog.DataType;
import minibase.query.QueryException;
import minibase.query.schema.Schema;

/**
 * Column reference predicate for retrieving value of certain column of tuples.
 *
 * @param <T>
 *           type of operand
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 * @version 1.0
 */
public class ColRef<T> extends Predicate<T> {

   /** schema of the tuples. */
   private final Schema schema;

   /** Field number. */
   private final int columnIndex;

   /** Data type of field. */
   private final DataType type;

   /**
    * Constructor returns instance of Column reference.
    *
    * @param schema
    *           tuple schema
    * @param columnIndex
    *           field number
    * @param type
    *           data type
    */
   public ColRef(final Schema schema, final int columnIndex, final DataType type) {
      this.schema = schema;
      this.columnIndex = columnIndex;
      this.type = schema.getColumn(columnIndex).getType();
      if (!this.type.equals(type)) {
         throw new IllegalArgumentException("Type mismatch: " + this.type + " != " + type);
      }
   }

   /**
    * Constructor returns instance of Column reference.
    *
    * @param schema
    *           tuple schema
    * @param fieldName
    *           field name
    * @param type
    *           data type
    */
   public ColRef(final Schema schema, final String fieldName, final DataType type) {
      this(schema, schema.getColumnIndex(fieldName), type);
   }

   /**
    * Constructor returns instance of Column reference.
    *
    * @param schema
    *           tuple schema
    * @param fieldNo
    *           field number
    */
   public ColRef(final Schema schema, final int fieldNo) {
      this(schema, fieldNo, schema.getColumn(fieldNo).getType());
   }

   /**
    * Constructor returns instance of Column reference.
    *
    * @param schema
    *           tuple schema
    * @param fieldName
    *           field name
    */
   public ColRef(final Schema schema, final String fieldName) {
      this(schema, schema.getColumnIndex(fieldName));
   }

   @Override
   public Operand<T> evaluate(final byte[] tuple) {
      return new Operand<T>(this.eval(tuple), this.type);
   }

   @Override
   public T eval(final byte[] tuple) {
      @SuppressWarnings("unchecked")
      final T value = (T) this.schema.getField(tuple, this.columnIndex);
      return value;
   }

   @Override
   public Optional<DataType> validate(final Schema schema) throws QueryException {
      final DataType schemaType = schema.getColumn(this.columnIndex).getType();
      if (schemaType != this.type) {
         throw new QueryException(this.toString() + ": Column reference doesn't contain expected type: "
               + this.type + ", but " + schemaType + " instead.");
      }
      return Optional.of(this.type);
   }

   @Override
   public String toString() {
      return String.format("%s: %s", this.type, this.columnIndex);
   }

   /**
    * Gets the referenced column index.
    * @return column index
    */
   public int getColumnIndex() {
      return this.columnIndex;
   }
}

/*
 * @(#)Schema.java   1.0   Feb 15, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import minibase.AtomicKeyType;
import minibase.CompositeKeyType;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.catalog.DataType;
import minibase.query.evaluator.predicate.ColRef;
import minibase.util.Convert;

/**
 * A schema is a collection of columns references and corresponding columns statistics. It also contains a
 * list of base table that appear in the schema. These base tables are used in the computation of the maximum
 * column unique cardinality.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public final class Schema implements Iterable<ColumnReference> {

   /** Minimum column width for output. */
   static final int MIN_WIDTH = 10;

   /** List of column references. */
   private final List<ColumnReference> columns;

   /** List of table references. */
   private final List<TableReference> tables;

   /** Candidate key of the described collection of tuples. */
   private final Collection<SchemaKey> candidateKeys;

   /** Foreign keys of the described collection of tuples. */
   private final Collection<SchemaForeignKey> foreignKeys;

   /** Relative offsets of the columns of the described collection of tuples. */
   private final int[] offsets;

   /** Total length of the columns of the described collection of tuples. */
   private final int length;

   /**
    * Constructs a schema with the given list of column references and the given list of corresponding column
    * statistics.
    *
    * @param columns
    *           list of column references
    * @param tables
    *           list of base table references
    * @param candidateKeys
    *           candidate keys of the described collection of tuples
    * @param foreignKeys
    *           foreign keys of the described collection of tuples
    */
   public Schema(final List<ColumnReference> columns, final List<TableReference> tables,
         final Collection<SchemaKey> candidateKeys, final Collection<SchemaForeignKey> foreignKeys) {
      this.columns = columns;
      this.tables = tables;
      this.candidateKeys = candidateKeys;
      this.foreignKeys = foreignKeys;
      this.offsets = new int[columns.size()];
      int offset = 0;
      for (int i = 0; i < this.offsets.length; i++) {
         this.offsets[i] = offset;
         offset += columns.get(i).getSize();
      }
      this.length = this.offsets[columns.size() - 1] + this.columns.get(columns.size() - 1).getSize();
   }

   /**
    * Checks whether the given column is contained in this schema.
    *
    * @param column
    *           column reference
    * @return {@code true} of the column is contained in this schema, {@code false} otherwise
    */
   public boolean containsColumn(final ColumnReference column) {
      return this.columns.contains(column);
   }

   /**
    * Returns a reference to the column with the given index.
    *
    * @param index
    *           column index
    * @return column reference
    */
   public ColumnReference getColumn(final int index) {
      return this.columns.get(index);
   }

   /**
    * Returns the index, i.e., the position, of the given column in this schema, or {@code -1} if the column
    * is not contained in the schema.
    *
    * @param column
    *           column reference
    * @return column index
    */
   public int getColumnIndex(final ColumnReference column) {
      return this.columns.indexOf(column);
   }

   /**
    * Returns the index, i.e., the position, of the column with the given name in this schema, or {@code -1}
    * if no such column is contained in the schema.
    *
    * @param name
    *           column name
    * @return column index
    */
   public int getColumnIndex(final String name) {
      for (int i = 0; i < this.columns.size(); i++) {
         final ColumnReference column = this.columns.get(i);
         if (column.getName().equalsIgnoreCase(name)) {
            return i;
         }
      }
      return -1;
   }

   /**
    * Returns the indexes, i.e., the positions, of the columns with the given names in this schema, or {@code -1}
    * if no such column is contained in the schema.
    *
    * @param names
    *           column names
    * @return column indexes
    */
   public int[] getColumnIndexes(final String... names) {
      final int[] columns = new int[names.length];
      int index = 0;
      for (final String name : names) {
         columns[index++] = this.getColumnIndex(name);
      }
      return columns;
   }

   /**
    * Returns the number of columns that are contained in this schema.
    *
    * @return column count
    */
   public int getColumnCount() {
      return this.columns.size();
   }

   /**
    * Returns the number of fields in the schema.
    *
    * @return the number of fields in the schema
    * @deprecated Use {@link Schema#getColumnCount()} instead.
    */
   @Deprecated
   public int getCount() {
      return this.columns.size();
   }

   @Override
   public Iterator<ColumnReference> iterator() {
      return this.columns.iterator();
   }

   /**
    * Checks whether the given table is contained in this schema.
    *
    * @param table
    *           table reference
    * @return {@code true} of the table is contained in this schema, {@code false} otherwise
    */
   public boolean containsTable(final TableReference table) {
      return this.tables.contains(table);
   }

   /**
    * Returns a reference to the table with the given index.
    *
    * @param index
    *           table index
    * @return base table reference
    */
   public TableReference getTable(final int index) {
      return this.tables.get(index);
   }

   /**
    * Returns the number of tables that are contained in this schema.
    *
    * @return table count
    */
   public int getTableCount() {
      return this.tables.size();
   }

   /**
    * Returns the candidate key of the tuple set described by these properties.
    *
    * @return candidate key
    */
   public Collection<SchemaKey> getCandidateKeys() {
      return Collections.unmodifiableCollection(this.candidateKeys);
   }

   /**
    * Returns all foreign keys defined in these logical properties as an immutable list.
    *
    * @return foreign keys of these logical properties
    */
   public Collection<SchemaForeignKey> getForeignKeys() {
      return Collections.unmodifiableCollection(this.foreignKeys);
   }

   /**
    * Checks whether the given key is contained in this schema, i.e., is a subset of the columns of this
    * schema.
    *
    * @param key
    *           key
    * @return {@code true} if the key is contained in this schema, {@code false} otherwise
    */
   public boolean containsKey(final SchemaKey key) {
      return key.isSubsetOf(this);
   }

   /**
    * Returns the size of a tuple in bytes.
    *
    * @return the size of a tuple in bytes
    */
   public int getLength() {
      return this.length;
   }

   /**
    * Creates a new tuple with this schema.
    *
    * @return new, uninitialized tuple
    */
   public byte[] newTuple() {
      return new byte[this.getLength()];
   }

   /**
    * Returns the type of the field with the given field number.
    *
    * @param name
    *           the name of the field
    * @return the type of the given field
    */
   public DataType fieldType(final String name) {
      return this.fieldType(this.fieldNumber(name));
   }
   /**
    * Returns the type of the field with the given field number.
    *
    * @param fieldNo
    *           the number of the field
    * @return the type of the given field
    * @deprecated Use {@link Schema#getColumn(int)} instead.
    */
   @Deprecated
   public DataType fieldType(final int fieldNo) {
      return this.getColumn(fieldNo).getType();
   }

   /**
    * Returns the length the field with the given field number.
    *
    * @param name
    *           the name of the field
    * @return the length of the given field
    */
   public int fieldLength(final String name) {
      return this.fieldLength(this.fieldNumber(name));
   }

   /**
    * Returns the length the field with the given field number.
    *
    * @param fieldNo
    *           the number of the field
    * @return the length of the given field
    * @deprecated Use {@link Schema#getColumn(int)} instead.
    */
   @Deprecated
   public int fieldLength(final int fieldNo) {
      return this.getColumn(fieldNo).getSize();
   }

   /**
    * Returns the offset the field with the given field number.
    *
    * @param name
    *           the name of the field
    * @return the offset of the given field
    */
   public int fieldOffset(final String name) {
      return this.fieldOffset(this.fieldNumber(name));
   }

   /**
    * Returns the offset the field with the given field number.
    *
    * @param fieldNo
    *           the number of the field
    * @return the offset of the given field
    * @deprecated Use {@link Schema#getColumnOffset(int)} instead.
    */
   @Deprecated
   public int fieldOffset(final int fieldNo) {
      return this.offsets[fieldNo];
   }

   /**
    * Returns the offset in bytes of the column with the given index.
    *
    * @param index
    *           column index
    * @return column offset
    */
   public int getColumnOffset(final int index) {
      return this.offsets[index];
   }

   /**
    * Gets the name the field with the given field number.
    *
    * @param fieldNo
    *           the number of the field
    * @return the name of the given field
    * @deprecated Use {@link Schema#getColumn(int)} instead.
    */
   @Deprecated
   public String fieldName(final int fieldNo) {
      return this.getColumn(fieldNo).getName();
   }

   /**
    * Get the alias the field with the given field number.
    *
    * @param fieldNo
    *           the number of the field
    * @return the alias of the given field
    * @deprecated Use {@link Schema#getColumn(int)} instead.
    */
   @Deprecated
   public String getFieldReferencedAlias(final int fieldNo) {
      final Optional<TableReference> parent = this.getColumn(fieldNo).getParent();
      if (parent.isPresent()) {
         return parent.get().getName();
      }
      return null;
   }

   /**
    * Returns the number of the field with the given name or {@code -1}, if not found.
    *
    * @param name
    *           the name of the field
    * @return the number of the field or {@code -1}, if not found
    * @deprecated Use {@link Schema#getColumnIndex(String)} instead.
    */
   @Deprecated
   public int fieldNumber(final String name) {
      return this.getColumnIndex(name);
   }

   /**
    * Returns the number of the field with the given name or {@code -1}, if not found.
    *
    * @param name
    *           the name of the field
    * @param tableAlias
    *           the table alias of the field
    * @return the number of the field or {@code -1}, if not found
    * @deprecated Use {@link Schema#getColumn(int)} instead.
    */
   @Deprecated
   public int fieldNumber(final String name, final String tableAlias) {
      if (tableAlias == null) {
         return this.fieldNumber(name);
      }
      for (int i = 0; i < this.columns.size(); i++) {
         final ColumnReference column = this.columns.get(i);
         if (column.getName().equals(name) && column.getParent().isPresent()
               && column.getParent().get().getName().equals(tableAlias)) {
            return i;
         }
      }
      return -1;
   }

   @Override
   public String toString() {
      return this.columns.toString();
   }

   /**
    * Appends the head of this schema to the given string builder.
    *
    * @param builder
    *           string builder
    */
   public void appendSchemaHeader(final StringBuilder builder) {
      // print and space the column names
      int len = 0;
      for (int i = 0; i < this.columns.size(); i++) {
         final ColumnReference column = this.columns.get(i);
         // print the column name
         builder.append(column.getName());
         // figure out the padding
         int collen = Math.max(column.getName().length(), Schema.MIN_WIDTH);
         // TODO do this for VARCHAR as well
         if (column.getType() == DataType.CHAR) {
            collen = Math.max(column.getSize(), collen);
         } else if (column.getType() == DataType.DATE) {
            len++;
            collen++;
         } else if (column.getType() == DataType.DATETIME) {
            collen = 22;
         }
         len += collen + 1;
         // pad the output to the field length
         for (int j = 0; j < collen - column.getName().length() + 1; j++) {
            builder.append(' ');
         }
      }
      // print the line separator
      builder.append("\n");
      for (int i = 0; i < len; i++) {
         builder.append('-');
      }
   }

   /**
    * Returns the parsed value of the field given by its number.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the given field
    * @return the parsed field value
    */
   public Object getField(final byte[] tuple, final int fieldNo) {
      // refer to the schema for the appropriate conversion
      switch (this.fieldType(fieldNo)) {
         case BIGINT:
            return Long.valueOf(this.getBigintField(tuple, fieldNo));
         case INT:
            return Integer.valueOf(this.getIntField(tuple, fieldNo));
         case SMALLINT:
            return Short.valueOf(this.getSmallintField(tuple, fieldNo));
         case TINYINT:
            return Byte.valueOf(this.getTinyintField(tuple, fieldNo));
         case DOUBLE:
            return Double.valueOf(this.getDoubleField(tuple, fieldNo));
         case FLOAT:
            return Float.valueOf(this.getFloatField(tuple, fieldNo));
         case DATE:
            return this.getDateField(tuple, fieldNo);
         case TIME:
            return this.getTimeField(tuple, fieldNo);
         case DATETIME:
            return this.getDatetimeField(tuple, fieldNo);
         case VARCHAR:
            throw new IllegalStateException("Varchar not yet supported.");
         case CHAR:
            return this.getCharField(tuple, fieldNo);
         default:
            throw new IllegalStateException("invalid attribute type");
      }
   }

   /**
    * Returns the parsed value of the field given by its name.
    *
    * @param tuple
    *           tuple contents
    * @param fieldName
    *           the name of the given field
    * @return the parsed field value
    */
   public Object getField(final byte[] tuple, final String fieldName) {
      return this.getField(tuple, this.fieldNumber(fieldName));
   }

   /**
    * Sets the field given by its number to the given value.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the given field
    * @param value
    *           the new field value.
    */
   public void setField(final byte[] tuple, final int fieldNo, final Object value) {
      if (value != null) {
         // refer to the schema for the appropriate conversion
         switch (this.fieldType(fieldNo)) {
            case BIGINT:
               this.setBigintField(tuple, fieldNo, ((Number) value).longValue());
               break;
            case INT:
               this.setIntField(tuple, fieldNo, ((Number) value).intValue());
               break;
            case SMALLINT:
               this.setSmallintField(tuple, fieldNo, ((Number) value).shortValue());
               break;
            case TINYINT:
               this.setTinyintField(tuple, fieldNo, ((Number) value).byteValue());
               break;
            case DOUBLE:
               this.setDoubleField(tuple, fieldNo, ((Number) value).doubleValue());
               break;
            case FLOAT:
               this.setFloatField(tuple, fieldNo, ((Number) value).floatValue());
               break;
            case DATE:
               this.setDateField(tuple, fieldNo, (Date) value);
               break;
            case TIME:
               this.setTimeField(tuple, fieldNo, (Time) value);
               break;
            case DATETIME:
               this.setDatetimeField(tuple, fieldNo, (Timestamp) value);
               break;
            case VARCHAR:
               throw new IllegalStateException("Varchar not yet supported.");
            case CHAR:
               this.setCharField(tuple, fieldNo, (String) value);
               break;
            default:
               throw new IllegalStateException("Unsupported data type: " + this.fieldType(fieldNo) + ".");
         }
      }
   }

   /**
    * Sets the field given by its number to the given value.
    *
    * @param tuple
    *           tuple contents
    * @param fieldName
    *           the name of the given field
    * @param value
    *           the new field value.
    */
   public void setField(final byte[] tuple, final String fieldName, final Object value) {
      this.setField(tuple, this.fieldNumber(fieldName), value);
   }

   /**
    * Returns an array of parsed values for all fields.
    *
    * @param tuple
    *           tuple contents
    * @return the parsed values for all fields.
    */
   public Object[] getAllFields(final byte[] tuple) {
      final Object[] values = new Object[this.getCount()];
      for (int i = 0; i < values.length; i++) {
         values[i] = this.getField(tuple, i);
      }
      return values;
   }

   /**
    * Sets all fields using the given values.
    *
    * @param tuple
    *           tuple contents
    * @param values
    *           the new field values
    */
   public void setAllFields(final byte[] tuple, final Object... values) {
      for (int i = 0; i < values.length; i++) {
         this.setField(tuple, i, values[i]);
      }
   }

   /**
    * Returns the value of a big Integer (long) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public long getBigintField(final byte[] tuple, final int fieldNo) {
      return Convert.readLong(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a big integer (long) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setBigintField(final byte[] tuple, final int fieldNo, final long value) {
      Convert.writeLong(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of an integer (int) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public int getIntField(final byte[] tuple, final int fieldNo) {
      return Convert.readInt(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of an integer (int) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setIntField(final byte[] tuple, final int fieldNo, final int value) {
      Convert.writeInt(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a small Integer (short) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public short getSmallintField(final byte[] tuple, final int fieldNo) {
      return Convert.readShort(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a small Integer (short) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setSmallintField(final byte[] tuple, final int fieldNo, final short value) {
      Convert.writeShort(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a tiny Integer (byte) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public byte getTinyintField(final byte[] tuple, final int fieldNo) {
      return Convert.readByte(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a tiny integer (byte) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setTinyintField(final byte[] tuple, final int fieldNo, final byte value) {
      Convert.writeByte(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a double field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public double getDoubleField(final byte[] tuple, final int fieldNo) {
      return Convert.readDouble(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a double field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setDoubleField(final byte[] tuple, final int fieldNo, final double value) {
      Convert.writeDouble(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a float field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public float getFloatField(final byte[] tuple, final int fieldNo) {
      return Convert.readFloat(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a float field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setFloatField(final byte[] tuple, final int fieldNo, final float value) {
      Convert.writeFloat(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a date field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public Date getDateField(final byte[] tuple, final int fieldNo) {
      return Convert.readDate(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a date field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setDateField(final byte[] tuple, final int fieldNo, final Date value) {
      Convert.writeDate(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a time field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public Time getTimeField(final byte[] tuple, final int fieldNo) {
      return Convert.readTime(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a time field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setTimeField(final byte[] tuple, final int fieldNo, final Time value) {
      Convert.writeTime(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a date time field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public Timestamp getDatetimeField(final byte[] tuple, final int fieldNo) {
      return Convert.readTimestamp(tuple, this.fieldOffset(fieldNo));
   }

   /**
    * Sets the value of a date time field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setDatetimeField(final byte[] tuple, final int fieldNo, final Timestamp value) {
      Convert.writeTimestamp(tuple, this.fieldOffset(fieldNo), value);
   }

   /**
    * Returns the value of a char (String) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @return the current value of the field
    */
   public String getCharField(final byte[] tuple, final int fieldNo) {
      return Convert.readString(tuple, this.fieldOffset(fieldNo), this.fieldLength(fieldNo));
   }

   /**
    * Sets the value of a char (String) field.
    *
    * @param tuple
    *           tuple contents
    * @param fieldNo
    *           the number of the field
    * @param value
    *           the new field value
    */
   public void setCharField(final byte[] tuple, final int fieldNo, final String value) {
      // truncate the string if too long
      final int len = this.fieldLength(fieldNo);
      // set the string and zero out the rest
      final int off = this.fieldOffset(fieldNo);
      Convert.writeString(tuple, off, value, len);
   }

   /**
    * Reads an integral value from the given field of a tuple at the given offset in an array.
    *
    * @param array
    *           to read from
    * @param offset
    *           offset of the tuple
    * @param fieldNo
    *           field number
    * @return integral value
    */
   public long getIntegral(final byte[] array, final int offset, final int fieldNo) {
      switch (this.getColumn(fieldNo).getType()) {
         case TINYINT:
            return Convert.readByte(array, offset + this.fieldOffset(fieldNo));
         case SMALLINT:
            return Convert.readShort(array, offset + this.fieldOffset(fieldNo));
         case INT:
            return Convert.readInt(array, offset + this.fieldOffset(fieldNo));
         case BIGINT:
            return Convert.readLong(array, offset + this.fieldOffset(fieldNo));
         default:
            throw new IllegalArgumentException("Field " + fieldNo + "is not integral.");
      }
   }

   /**
    * Reads a floating-point value from the given field of a tuple at the given offset in an array.
    *
    * @param array
    *           to read from
    * @param offset
    *           offset of the tuple
    * @param fieldNo
    *           field number
    * @return floating-point value
    */
   public double getFloating(final byte[] array, final int offset, final int fieldNo) {
      switch (this.getColumn(fieldNo).getType()) {
         case TINYINT:
            return Convert.readByte(array, offset + this.fieldOffset(fieldNo));
         case SMALLINT:
            return Convert.readShort(array, offset + this.fieldOffset(fieldNo));
         case INT:
            return Convert.readInt(array, offset + this.fieldOffset(fieldNo));
         case BIGINT:
            return Convert.readLong(array, offset + this.fieldOffset(fieldNo));
         case FLOAT:
            return Convert.readFloat(array, offset + this.fieldOffset(fieldNo));
         case DOUBLE:
            return Convert.readDouble(array, offset + this.fieldOffset(fieldNo));
         default:
            throw new IllegalArgumentException("Field " + fieldNo + "is not numeric.");
      }
   }

   /**
    * Reads a string from the given field of a tuple at the given offset in an array.
    *
    * @param array
    *           to read from
    * @param offset
    *           offset of the tuple
    * @param fieldNo
    *           field number
    * @return string value
    */
   public String getString(final byte[] array, final int offset, final int fieldNo) {
      return Convert.readString(array, offset + this.fieldOffset(fieldNo), this.fieldLength(fieldNo));
   }

   /**
    * Reads a date-time value from the given field of a tuple at the given offset in an array.
    *
    * @param array
    *           to read from
    * @param offset
    *           offset of the tuple
    * @param fieldNo
    *           field number
    * @return date-time value
    */
   public Timestamp getDateTime(final byte[] array, final int offset, final int fieldNo) {
      if (this.getColumn(fieldNo).getType() == DataType.DATE) {
         return new Timestamp(Convert.readDate(array, offset + this.fieldOffset(fieldNo)).getTime());
      } else {
         return Convert.readTimestamp(array, offset + this.fieldOffset(fieldNo));
      }
   }

   /**
    * Reads a time value from the given field of a tuple at the given offset in an array.
    *
    * @param array
    *           to read from
    * @param offset
    *           offset of the tuple
    * @param fieldNo
    *           field number
    * @return time value
    */
   public Time getTime(final byte[] array, final int offset, final int fieldNo) {
      return Convert.readTime(array, offset + this.fieldOffset(fieldNo));
   }

   /**
    * returns a string representation of the given tuple.
    *
    * @param tuple
    *           tuple to show
    * @return string representation
    */
   public String showTuple(final byte[] tuple) {
      final StringBuilder sb = new StringBuilder();
      final int cnt = this.getCount();

      for (int i = 0; i < cnt; i++) {
         final String str;
         final int len;
         switch (this.fieldType(i)) {
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
               str = String.format(Locale.ENGLISH, "%-" + Schema.MIN_WIDTH + "d", this.getField(tuple, i));
               sb.append(str);
               len = 0;
               break;
            case DOUBLE:
            case FLOAT:
               str = String.format(Locale.ENGLISH, "%-" + Schema.MIN_WIDTH + "s", this.getField(tuple, i));
               if (str.length() > Schema.MIN_WIDTH) {
                  // number too long
                  final String[] parts = str.split("E");
                  if (parts.length == 2) {
                     // e format
                     sb.append(parts[0].substring(0, Schema.MIN_WIDTH - parts[1].length() - 1));
                     sb.append('E');
                     sb.append(parts[1]);
                  } else {
                     sb.append(parts[0].substring(0, Schema.MIN_WIDTH));
                  }
               } else {
                  sb.append(str);
               }
               len = 0;
               break;
            case DATE:
               str = this.getDateField(tuple, i).toString();
               sb.append(str);
               len = Schema.MIN_WIDTH - str.length();
               break;
            case TIME:
               str = this.getTimeField(tuple, i).toString();
               sb.append(str);
               len = Schema.MIN_WIDTH - str.length();
               break;
            case DATETIME:
               str = this.getDatetimeField(tuple, i).toString();
               sb.append(str);
               len = Schema.MIN_WIDTH - str.length();
               break;
            case VARCHAR:
               throw new IllegalStateException("Varchar not yet supported.");
            case CHAR:
            default:
               str = this.getCharField(tuple, i);
               sb.append(str);
               len = Math.max(Schema.MIN_WIDTH, this.fieldLength(i)) - str.length();
               break;
         }
         for (int j = 0; j < len + 1; j++) {
            sb.append(' ');
         }
      }
      return sb.toString();
   }

   /**
    * Generates a hash this tuple by xor-ing all values.
    *
    * @param tuple
    *           tuple contents
    * @param salt
    *           salt to create different hashes
    * @return hash
    */
   public int hash(final byte[] tuple, final int salt) {
      final int[] fields = new int[this.getCount()];
      for (int i = 0; i < fields.length; i++) {
         fields[i] = i;
      }
      return this.hash(tuple, fields, salt);
   }

   /**
    * Generates a hash this tuple and the field numbers by xor-ing the values.
    *
    * @param tuple
    *           tuple contents
    * @param fields
    *           field numbers
    * @param salt
    *           salt to create different hashes
    * @return hash
    */
   public int hash(final byte[] tuple, final int[] fields, final int salt) {
      int hash = 0;
      for (final int field : fields) {
         final DataType type = this.fieldType(field);
         switch (type) {
            case BIGINT:
               final long lField = this.getBigintField(tuple, field);
               hash ^= (int) lField;
               hash ^= (int) (lField >>> 32);
               break;
            case INT:
               hash ^= this.getIntField(tuple, field);
               break;
            case SMALLINT:
               hash ^= this.getSmallintField(tuple, field);
               break;
            case TINYINT:
               hash ^= this.getTinyintField(tuple, field);
               break;
            case DOUBLE:
               final long dField = this.getBigintField(tuple, field);
               hash ^= (int) dField;
               hash ^= (int) (dField >>> 32);
               break;
            case FLOAT:
               hash ^= this.getIntField(tuple, field);
               break;
            case DATE:
               final long daField = this.getDateField(tuple, field).getTime();
               hash ^= (int) daField;
               hash ^= (int) (daField >>> 32);
               break;
            case TIME:
               final long tField = this.getTimeField(tuple, field).getTime();
               hash ^= (int) tField;
               hash ^= (int) (tField >>> 32);
               break;
            case DATETIME:
               final long tsField = this.getDatetimeField(tuple, field).getTime();
               hash ^= (int) tsField;
               hash ^= (int) (tsField >>> 32);
               break;
            case CHAR:
               hash ^= this.getCharField(tuple, field).hashCode();
               break;
            case VARCHAR:
            default:
               throw new UnsupportedOperationException(
                     "The type:\"" + type.toString() + "\" is not supported.");
         }
         hash += salt;
         hash *= 7;
      }

      return hash;
   }

   /**
    * Builds and returns a new tuple resulting from joining two tuples.
    *
    * @param t1
    *           the left tuple
    * @param t2
    *           the right tuple
    * @return the tuple resulting from the join of the two given tuples
    */
   public static byte[] join(final byte[] t1, final byte[] t2) {
      final byte[] res = new byte[t1.length + t2.length];
      System.arraycopy(t1, 0, res, 0, t1.length);
      System.arraycopy(t2, 0, res, t1.length, t2.length);
      return res;
   }

   /**
    * Returns a new schema that contains the union of the columns of the given left and right schema. Note
    * that this method disregards the tables and keys contained in the original schemas.
    *
    * @param left
    *           left columns
    * @param right
    *           right columns
    * @return joined schema
    */
   public static Schema join(final Schema left, final Schema right) {
      final List<ColumnReference> columns = new ArrayList<>();
      for (final ColumnReference column : left) {
         columns.add(column);
      }
      for (final ColumnReference column : right) {
         columns.add(column);
      }
      return new Schema(columns, Collections.emptyList(), Collections.emptySet(), Collections.emptySet());
   }

   /**
    * Returns a new schema that contains the projected columns specified by their indexes.
    *
    * @param schema
    *           original schema
    * @param columns
    *           column indexes
    * @return projected schema
    */
   public static Schema project(final Schema schema, final int[] columns) {
      if (columns == null || columns.length == 0) {
         throw new IllegalArgumentException("Projection list cannot be empty.");
      }
      final List<ColumnReference> result = new ArrayList<>();
      for (final int i : columns) {
         result.add(schema.getColumn(i));
      }
      return new Schema(result, Collections.emptyList(), Collections.emptySet(), Collections.emptySet());
   }

   /**
    * Gets SearchKeyType of the given fields.
    * @param fields the fields
    * @return the search key type
    */
   public SearchKeyType searchKeyType(final int... fields) {

      if (fields.length == 1) {
         final int field = fields[0];
         final DataType type = this.fieldType(field);
         final int length = this.fieldLength(field);
         return SearchKeyType.get(type, length);
      }

      final AtomicKeyType[] types = new AtomicKeyType[fields.length];
      int index = 0;
      for (final int field : fields) {
         final DataType type = this.fieldType(field);
         final int length = this.fieldLength(field);
         types[index] = SearchKeyType.get(type, length);
         index++;
      }

      return CompositeKeyType.getInstance(types);
   }

   /**
    * Gets the composite SearchKey of the given fields.
    * @param tuple the tuple
    * @param fields the fields
    * @return the search key
    */
   public SearchKey searchKey(final byte[] tuple, final int... fields) {
      if (fields.length == 1) {
         return this.searchKey(tuple, fields[0]);
      }

      final AtomicKeyType[] types = new AtomicKeyType[fields.length];
      final Object[] values = new Object[fields.length];

      int index = 0;
      for (final int field : fields) {
         final DataType type = this.fieldType(field);
         final int length = this.fieldLength(field);
         types[index] = SearchKeyType.get(type, length);
         values[index] = this.getField(tuple, field);
         index++;
      }

      final CompositeKeyType keyType = CompositeKeyType.getInstance(types);
      return new SearchKey(keyType, values);
   }

   /**
    * Gets the composite SearchKey of the given fields.
    * @param tuple the tuple
    * @param type type of the composite key
    * @param fields the fields
    * @return the search key
    */
   public SearchKey searchKey(final byte[] tuple, final SearchKeyType type, final int... fields) {
      final Object[] values = new Object[fields.length];

      int index = 0;
      for (final int field : fields) {
         values[index] = this.getField(tuple, field);
         index++;
      }

      return new SearchKey(type, values);
   }

   /**
    * Gets the atomic SearchKey of the given field.
    * @param tuple the tuple
    * @param field the field
    * @return the search key
    */
   public SearchKey searchKey(final byte[] tuple, final int field) {
      final DataType type = this.fieldType(field);
      final int length = this.fieldLength(field);
      final Object value = this.getField(tuple, field);
      return new SearchKey(SearchKeyType.get(type, length), value);
   }

   /**
    * Creates a ColRef predicate to the given column.
    * @param column column index
    * @return ColRef
    */
   public ColRef<?> getRefPredicate(final int column) {
      return new ColRef<>(this, column);
   }
}

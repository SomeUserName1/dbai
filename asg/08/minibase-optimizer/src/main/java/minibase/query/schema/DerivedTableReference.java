/*
 * @(#)DerivedTableReference.java   1.0   Jun 11, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.List;
import java.util.Optional;

/**
 * A reference to a derived table that is dynamically created by the query, for example by a subquery.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class DerivedTableReference extends AbstractReference implements TableReference {

   /** Name of this derived table reference. */
   private final String name;

   /** Columns of this derived table reference. */
   private final List<ColumnReference> columns;

   /** Declared size of this derived table in bytes. */
   private final int size;

   /** Declared width of this derived table as a fraction of a block. */
   private final double width;

   /**
    * Creates a new derived table reference with the given name and columns.
    *
    * @param id
    *           ID of this reference
    * @param name
    *           name of this reference
    * @param columns
    *           table columns
    */
   DerivedTableReference(final int id, final String name, final List<ColumnReference> columns) {
      super(id);
      this.name = name;
      this.columns = columns;
      int size = 0;
      double width = 0.0;
      for (final ColumnReference column : columns) {
         size += column.getSize();
         width += column.getWidth();
      }
      this.size = size;
      this.width = width;
   }

   @Override
   public String getName() {
      return this.name;
   }

   @Override
   public Optional<ColumnReference> getColumn(final String name) {
      for (final ColumnReference column : this.columns) {
         if (column.getName().equals(name)) {
            return Optional.of(column);
         }
      }
      return Optional.empty();
      // throw new IllegalStateException("Column " + name + " is undefined for table " + this.name + ".");
   }

   @Override
   public List<ColumnReference> getColumns() {
      return this.columns;
   }

   @Override
   public int getSize() {
      return this.size;
   }

   @Override
   public double getWidth() {
      return this.width;
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder("(");
      result.append(this.getID());
      result.append(")->");
      result.append(this.name);
      result.append(this.columns);
      return result.toString();
   }
}

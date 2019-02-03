/*
 * @(#)StoredColumnReference.java   1.0   Aug 23, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.Optional;

import minibase.catalog.ColumnDescriptor;
import minibase.catalog.DataType;

/**
 * A reference to a column object in the system catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class StoredColumnReference extends AbstractReference implements ColumnReference {

   /** Column descriptor of this column reference. */
   private final ColumnDescriptor column;

   /** Pointer to parent table of this column reference. */
   private final TableReference parent;

   /**
    * Constructs a new column reference for the given system catalog and with the given reference ID that
    * points to the given catalog ID through the given name.
    *
    * @param id
    *           ID of this reference
    * @param column
    *           column descriptor of this reference
    * @param parent
    *           parent reference of this reference
    */
   StoredColumnReference(final int id, final ColumnDescriptor column, final TableReference parent) {
      super(id);
      this.column = column;
      this.parent = parent;
   }

   @Override
   public String getName() {
      return this.column.getName();
   }

   /**
    * Returns the full name of the column.
    *
    * @return column name
    */
   public String getFullName() {
      return this.parent != null ? this.parent.getName() + "." + this.getName() : this.getName();
   }

   /**
    * Returns the column descriptor that describes this stored column.
    *
    * @return column descriptor
    */
   public ColumnDescriptor getDescriptor() {
      return this.column;
   }

   /**
    * Returns the reference to the parent table of this column reference.
    *
    * @return parent reference or {@code null}, if this column reference has no parent table
    */
   @Override
   public Optional<TableReference> getParent() {
      return Optional.of(this.parent);
   }

   @Override
   public DataType getType() {
      return this.column.getType();
   }

   @Override
   public int getSize() {
      return this.column.getSize();
   }

   @Override
   public double getWidth() {
      return this.column.getWidth();
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      if (this.parent != null) {
         result.append(this.parent.getName());
      }
      result.append(".");
      result.append(this.getName());
      return result.toString();
   }
}

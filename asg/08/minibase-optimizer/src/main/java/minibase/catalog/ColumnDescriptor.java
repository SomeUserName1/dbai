/*
 * @(#)ColumnDescriptor.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Descriptor for a column in the Minibase system catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public final class ColumnDescriptor extends NamedDescriptor implements OwnedDescriptor {

   /** Type of the described column. */
   private final DataType type;

   /** Size in bytes of the described column. */
   private final int size;

   /** Identifier of the owner table. */
   private final int ownerID;

   /** Statistics that describe the data in this column. */
   private final ColumnStatistics statistics;

   /**
    * Constructs a new column for a column with the given globally unique ID, column name, column type and
    * enclosing table.
    *
    * @param catalog
    *           system catalog of this column
    * @param statistics
    *           statistics of this column
    * @param id
    *           globally unique id of this column
    * @param name
    *           name of this column
    * @param type
    *           type of this column
    * @param size
    *           size of this column in bytes
    * @param ownerID
    *           identifier of the owner table
    */
   ColumnDescriptor(final SystemCatalog catalog, final int id, final String name, final DataType type,
         final int size, final int ownerID, final ColumnStatistics statistics) {
      super(catalog, id, name);
      this.type = type;
      this.size = size;
      this.ownerID = ownerID;
      this.statistics = statistics;
   }

   /**
    * Returns a set of statistical values that characterize the data in the column described by this column
    * descriptor.
    *
    * @return columns statistics
    */
   public ColumnStatistics getStatistics() {
      return this.statistics;
   }

   /**
    * Returns the column data type of this column descriptor.
    *
    * @return column data type
    */
   public DataType getType() {
      return this.type;
   }

   /**
    * Returns the declared size of the values of the column in bytes.
    *
    * @return declared value size (in bytes)
    */
   public int getSize() {
      return this.size;
   }

   /**
    * Returns the declared size of the values of the column as a fraction of a block. This is a property of
    * the schema and not to be confused with the <em>average width</em> of values in the column, which can be
    * accessed using {@link ColumnDescriptor#getStatistics()}.
    *
    * @return declared value width (as a fraction of a block)
    */
   public double getWidth() {
      return (double) this.size / this.getSystemCatalog().getPageSize();
   }

   @Override
   public int getOwnerID() {
      return this.ownerID;
   }

   /**
    * Returns the primary key constraint defined on this column or {@code null} if there is none.
    *
    * @return primary key descriptor or {@code null}, if there is none
    */
   public PrimaryKeyDescriptor getPrimaryKey() {
      return this.getSystemCatalog().getPrimaryKey(this.getOwnerID(), this.getCatalogID());
   }

   /**
    * Returns all foreign keys defined for this column as an immutable collection.
    *
    * @return foreign keys of this column
    */
   public Collection<ForeignKeyDescriptor> getForeignKeys() {
      return this.getSystemCatalog().getForeignKeys(this.getOwnerID(), this.getCatalogID());
   }

   @Override
   public String toSQL() {
      final StringBuilder result = new StringBuilder();
      result.append(String.format("%1$-10s ", this.getName()));
      result.append(this.getType().name());
      if (this.getType().getSize() < 0) {
         result.append("(");
         result.append(this.getSize());
         result.append(")");
      }
      for (final ConstraintDescriptor constraint : this.getColumnConstraintDescriptors()) {
         result.append(" ");
         result.append(constraint.toSQL());
      }
      return result.toString();
   }

   /**
    * Returns all column constraints that have been defined for this column descriptor as an unmodifiable
    * list.
    *
    * @return unmodifiable list of constraint descriptors
    */
   private Collection<ConstraintDescriptor> getColumnConstraintDescriptors() {
      return Collections.unmodifiableCollection(this.getSystemCatalog().getConstraints(this.getOwnerID())
            .stream().filter(p -> !p.isTableConstraint() && p.getColumnID() == this.getCatalogID())
            .collect(Collectors.<ConstraintDescriptor> toSet()));
   }
}

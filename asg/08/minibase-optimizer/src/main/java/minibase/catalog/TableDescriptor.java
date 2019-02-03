/*
 * @(#)TableDescriptor.java   1.0   Jan 3, 2014
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import minibase.query.schema.Schema;
import minibase.query.schema.SchemaBuilder;

/**
 * Descriptor for a table in the Minibase system catalog. A table descriptor provides read-only access to the
 * table's columns, constraints, and indexes. It can also be used to access the statistics of the table.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public final class TableDescriptor extends NamedDescriptor {

   /** Order of the data stored in the table. */
   private final DataOrder dataOrder;

   /** Average size of tuples of the table in bytes. */
   private int size;

   /** Statistics that describe the data in the table. */
   private final TableStatistics statistics;

   /**
    * Constructs a new table descriptor for a table with the given name and the given identifier.
    *
    * @param catalog
    *           system catalog of the table
    * @param id
    *           globally unique ID of the table
    * @param name
    *           name of the table
    * @param order
    *           data order of the table
    * @param size
    *           average tuples size of the table in bytes.
    * @param statistics
    *           statistics of the table
    */
   TableDescriptor(final SystemCatalog catalog, final int id, final String name, final DataOrder order,
         final int size, final TableStatistics statistics) {
      super(catalog, id, name);
      this.dataOrder = order;
      this.size = size;
      this.statistics = statistics;
   }

   /**
    * Returns the file type used to store the data of the table.
    *
    * @return file type of the table
    */
   public DataOrder getDataOrder() {
      return this.dataOrder;
   }

   /**
    * Returns the declared size of tuple in the table in bytes.
    *
    * @return declared tuple size (in bytes)
    */
   public int getSize() {
      return this.size;
   }

   /**
    * Returns the declared width of the tuples in the table as a fraction of a block. This is a property of
    * the schema and not to be confused with the <em>average width</em> of tuples in the table, which can be
    * accessed using {@link TableDescriptor#getStatistics()}.
    *
    * @return declared tuple width (as a fraction of a block)
    */
   public double getWidth() {
      return (double) this.size / this.getSystemCatalog().getPageSize();
   }

   /**
    * Returns a set of statistical values that characterize the data in the table described by this table
    * descriptor.
    *
    * @return table statistics
    */
   public TableStatistics getStatistics() {
      return this.statistics;
   }

   /**
    * Checks if this table has a column with the given name.
    *
    * @param name
    *           column name
    * @return {@code true} if a column with the given name exists, {@code false} otherwise
    */
   public boolean hasColumn(final String name) {
      return this.getSystemCatalog().hasColumn(this.getCatalogID(), name);
   }

   /**
    * Returns a column of this table given its name or throws an {@link IllegalStateException}, if this table
    * has no column with the given name.
    *
    * @param name
    *           column name
    * @return column
    */
   public ColumnDescriptor getColumn(final String name) {
      return this.getSystemCatalog().getColumn(this.getCatalogID(), name);
   }

   /**
    * Returns all columns of the table as an immutable list.
    *
    * @return columns of the table
    */
   public List<ColumnDescriptor> getColumns() {
      return this.getSystemCatalog().getColumns(this.getCatalogID());
   }

   /**
    * Returns the index key of the table, if its file type is {@link DataOrder#SORTED} or
    * {@link DataOrder#HASHED}, {@code null} otherwise.
    *
    * @return index key of the table
    */
   public IndexKeyDescriptor getIndexKey() {
      return this.getSystemCatalog().getIndexKey(this.getCatalogID());
   }

   /**
    * Returns all (non-bitmap) indexes defined for the table as an immutable list.
    *
    * @return indexes of the table
    */
   public Collection<IndexDescriptor> getIndexes() {
      return this.getSystemCatalog().getIndexes(this.getCatalogID());
   }

   /**
    * Returns all bitmap indexes defined for the table as an immutable list.
    *
    * @return bitmap indexes of the table
    */
   public Collection<BitmapIndexDescriptor> getBitmapIndexes() {
      return this.getSystemCatalog().getBitmapIndexes(this.getCatalogID());
   }

   /**
    * Returns the primary key defined for this table.
    *
    * @return primary key of the table
    */
   public PrimaryKeyDescriptor getPrimaryKey() {
      return this.getSystemCatalog().getPrimaryKey(this.getCatalogID());
   }

   /**
    * Returns all foreign keys defined for the table as an immutable collection.
    *
    * @return foreign keys of the table
    */
   public Collection<ForeignKeyDescriptor> getForeignKeys() {
      return this.getSystemCatalog().getForeignKeys(this.getCatalogID());
   }

   /**
    * Returns the columns IDs of the candidate key of the table.
    *
    * @return column IDs of the candidate key
    */
   public Collection<CatalogKey> getCandidateKeys() {
      // TODO This could be extended to include unique keys.
      final Set<CatalogKey> keySet = new HashSet<>();
      final PrimaryKeyDescriptor primaryKey = this.getPrimaryKey();
      if (primaryKey != null) {
         keySet.add(primaryKey.getKey());
      }
      return Collections.unmodifiableCollection(keySet);
   }

   /**
    * Translates this table descriptor to an instance of {@link minibase.query.evaluator.Schema}.
    *
    * @return schema of the table
    */
   public Schema getEvaluatorSchema() {
      final List<ColumnDescriptor> columns = this.getColumns();
      final SchemaBuilder builder = new SchemaBuilder();
      for (int i = 0; i < columns.size(); i++) {
         final ColumnDescriptor column = columns.get(i);
         builder.addField(column.getName(), column.getType(), column.getSize());
      }
      return builder.build();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toSQL() {
      final StringBuilder result = new StringBuilder("CREATE TABLE ");
      result.append(this.getName());
      result.append(" (\n");
      formatCollection(result, this.getColumns(), "   ", this.getTableConstraintDescriptors().size() > 0);
      formatCollection(result, this.getTableConstraintDescriptors(), "   ", false);
      result.append(");");
      return result.toString();
   }

   /**
    * Appends the given list to the given buffer. Each entry of the list is printed on a new line and indented
    * by the given indent. Optionally, the comma after the last entry can be printed or skipped.
    *
    * @param result
    *           string builder to which the list is appended
    * @param descriptors
    *           list of descriptors to format
    * @param indent
    *           indent for each line
    * @param lastComma
    *           {@code true} if a comma should be printed after the last entry, {@code false} otherwise
    */
   private static void formatCollection(final StringBuilder result,
         final Collection< ? extends Descriptor> descriptors, final String indent, final boolean lastComma) {
      int i = 0;
      for (final Descriptor descriptor : descriptors) {
         result.append(indent);
         result.append(descriptor.toSQL());
         if (i + 1 < descriptors.size() || lastComma) {
            result.append(",\n");
         } else {
            result.append("\n");
         }
         i++;
      }
   }

   /**
    * Returns all table constraints that have been defined for this table descriptor as an unmodifiable list.
    *
    * @return unmodifiable list of constraint descriptors
    */
   private Collection<ConstraintDescriptor> getTableConstraintDescriptors() {
      return Collections.unmodifiableCollection(this.getSystemCatalog().getConstraints(this.getCatalogID())
            .stream().filter(d -> d.isTableConstraint()).collect(Collectors.<ConstraintDescriptor> toSet()));
   }

   /**
    * Returns the size of this table as the sum of all column sizes.
    */
   void updateSize() {
      int size = 0;
      for (final ColumnDescriptor column : this.getColumns()) {
         size += column.getSize();
      }
      this.size = size;
   }
}

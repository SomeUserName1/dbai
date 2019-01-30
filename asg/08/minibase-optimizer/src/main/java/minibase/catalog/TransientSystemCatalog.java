/*
 * @(#)TransientSystemCatalog.java   1.0   Jan 3, 2014
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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import minibase.query.optimizer.Expression;
import minibase.storage.file.DiskManager;

/**
 * Transient implementation of the Minibase system catalog for testing and simulation purposes.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public final class TransientSystemCatalog extends AbstractSystemCatalog implements SystemCatalog {

   /** Page size used by this system catalog in bytes. */
   private final int pageSize;

   /** ID of the next system catalog element. */
   private int nextID;

   /**
    * Constructs a new transient system catalog and initializes all internal data structures.
    */
   public TransientSystemCatalog() {
      this(DiskManager.PAGE_SIZE);
   }

   /**
    * Constructs a new transient system catalog and initializes all internal data structures.
    *
    * @param pageSize
    *           page size in bytes
    */
   public TransientSystemCatalog(final int pageSize) {
      this.nextID = 0;
      this.pageSize = pageSize;
   }

   /**
    * Returns the ID that is assigned to the next catalog element.
    *
    * @return next ID
    */
   private int getNextID() {
      return this.nextID++;
   }

   @Override
   public int getPageSize() {
      return this.pageSize;
   }

   @Override
   public int createTable(final TableStatistics statistics, final String name, final DataOrder order) {
      if (this.getDescriptor(TableDescriptor.class, name).isPresent()) {
         throw new IllegalStateException("Table " + name + " exists already.");
      }
      final int tableID = this.getNextID();
      final TableDescriptor table = new TableDescriptor(this, tableID, name, order, 0, statistics);
      this.putDescriptor(table);
      return tableID;
   }

   @Override
   public int createTable(final TableStatistics statistics, final String name) {
      return this.createTable(statistics, name, DataOrder.ANY);
   }

   @Override
   public void dropTable(final int tableID) {
      final Optional<TableDescriptor> table = this.getDescriptor(TableDescriptor.class, tableID);
      if (table.isPresent()) {
         this.dropTable(table.get());
      } else {
         throw new IllegalStateException("Table " + tableID + " does not exist.");
      }
   }

   @Override
   public void dropTable(final String name) {
      final Optional<TableDescriptor> table = this.getDescriptor(TableDescriptor.class, name);
      if (table.isPresent()) {
         this.removeDescriptor(table.get());
      } else {
         throw new IllegalStateException("Table " + name + " does not exist.");
      }
   }

   /**
    * Drops a table, its columns, its indexes, and its constraints from the system catalog.
    *
    * @param table
    *           table descriptor
    */
   private void dropTable(final TableDescriptor table) {
      // Drop table columns
      for (final ColumnDescriptor column : table.getColumns()) {
         this.dropColumn(column);
      }
      // Drop table indexes
      for (final IndexDescriptor index : table.getIndexes()) {
         this.dropIndex(index);
      }
      // Drop table bitmap indexes
      for (final BitmapIndexDescriptor index : table.getBitmapIndexes()) {
         this.dropBitmapIndex(index);
      }
      // Drop table index key
      final IndexKeyDescriptor indexKey = this.getIndexKey(table.getCatalogID());
      if (indexKey != null) {
         this.dropIndexKey(indexKey);
      }
      // Drop table primary key
      final PrimaryKeyDescriptor primaryKey = table.getPrimaryKey();
      if (primaryKey != null) {
         this.dropPrimaryKey(primaryKey);
      }
      // Drop table foreign keys
      for (final ForeignKeyDescriptor foreignKey : table.getForeignKeys()) {
         this.dropForeignKey(foreignKey);
      }
      // Drop table
      this.removeDescriptor(table);
   }

   @Override
   public TableDescriptor getTableDescriptor(final int tableID) {
      return this.getDescriptor(TableDescriptor.class, tableID).orElseThrow(
            () -> new IllegalStateException("ID " + tableID + " does not reference a table."));
   }

   @Override
   public boolean hasTable(final String name) {
      return this.getDescriptor(TableDescriptor.class, name).isPresent();
   }

   @Override
   public TableDescriptor getTable(final String name) {
      return this.getDescriptor(TableDescriptor.class, name).orElseThrow(
            () -> new IllegalStateException("Table " + name + " does not exist."));
   }

   @Override
   public Collection<TableDescriptor> getTables() {
      return this.getDescriptors(TableDescriptor.class);
   }

   @Override
   public int createColumn(final ColumnStatistics statistics, final String name, final DataType type,
         final int size, final int tableID) {
      for (final ColumnDescriptor descriptor : this.getColumns(tableID)) {
         if (descriptor.getName().equals(name)) {
            throw new IllegalStateException("Column " + name + " exists already.");
         }
      }
      final int actualSize;
      if (type.getSize() < 0) {
         if (size > 0) {
            actualSize = size;
         } else {
            throw new IllegalArgumentException("The size of a column must be greater or equal to 1.");
         }
      } else {
         actualSize = type.getSize();
      }
      // Create new column descriptor and add it to the cache
      final int columnID = this.getNextID();
      final ColumnDescriptor column = new ColumnDescriptor(this, columnID, name, type, actualSize, tableID,
            statistics);
      this.putDescriptor(column);
      // Invalidate cached parent descriptor
      final TableDescriptor table = this.getTableDescriptor(tableID);
      table.updateSize();
      return columnID;
   }

   /**
    * Drops a column and its constrains from the system catalog.
    *
    * @param column
    *           column descriptor
    */
   private void dropColumn(final ColumnDescriptor column) {
      final PrimaryKeyDescriptor primaryKey = column.getPrimaryKey();
      if (primaryKey != null) {
         this.dropPrimaryKey(primaryKey);
      }
      for (final ForeignKeyDescriptor foreignKey : column.getForeignKeys()) {
         this.dropForeignKey(foreignKey);
      }
      this.removeDescriptor(column);
   }

   @Override
   public ColumnDescriptor getColumnDescriptor(final int columnID) {
      return this.getDescriptor(ColumnDescriptor.class, columnID).orElseThrow(
            () -> new IllegalStateException("ID " + columnID + " does not reference a column."));
   }

   @Override
   public boolean hasColumn(final int tableID, final String name) {
      return this.getDescriptor(ColumnDescriptor.class, tableID, name).isPresent();
   }

   @Override
   public ColumnDescriptor getColumn(final int tableID, final String name) {
      return this.getDescriptor(ColumnDescriptor.class, tableID, name).orElseThrow(
            () -> new IllegalStateException("Column " + name + " does not exist."));
   }

   @Override
   public List<ColumnDescriptor> getColumns(final int tableID) {
      return this.getDescriptors(ColumnDescriptor.class, tableID);
   }

   @Override
   public ConstraintDescriptor getConstraintDescriptor(final int constraintID) {
      return this.getDescriptor(ConstraintDescriptor.class, constraintID).orElseThrow(
            () -> new IllegalStateException("ID " + constraintID + " does not reference a constraint."));
   }

   @Override
   public boolean hasConstraint(final String name) {
      return this.getDescriptor(ConstraintDescriptor.class, name).isPresent();
   };

   @Override
   public ConstraintDescriptor getConstraint(final String name) {
      return this.getDescriptor(ConstraintDescriptor.class, name).orElseThrow(
            () -> new IllegalStateException("Constraint " + name + " does not exist."));
   }

   @Override
   public Collection<ConstraintDescriptor> getConstraints(final int tableID) {
      return this.getDescriptors(ConstraintDescriptor.class, tableID);
   }

   @Override
   public int createTablePrimaryKey(final String name, final int tableID, final int[] columnIDs) {
      // check if name is valid
      this.assertConstraintNotExists(name);
      // check if tableID is valid
      this.getTableDescriptor(tableID);
      // check if attributeIDs are valid
      for (final int id : columnIDs) {
         this.getColumnDescriptor(id);
      }
      // everything is okay, create the new constraint
      final int constraintID = this.getNextID();
      final PrimaryKeyDescriptor primaryKey = new PrimaryKeyDescriptor(this, constraintID, name, tableID,
            columnIDs);
      this.putDescriptor(ConstraintDescriptor.class, primaryKey);
      this.putDescriptor(primaryKey);
      return constraintID;
   }

   @Override
   public int createColumnPrimaryKey(final String name, final int tableID, final int columnID) {
      // check if name is valid
      this.assertConstraintNotExists(name);
      // check if attributeID is valid
      this.getColumnDescriptor(columnID);
      // everything is okay, create the new constraint
      final int constraintID = this.getNextID();
      final PrimaryKeyDescriptor primaryKey = new PrimaryKeyDescriptor(this, constraintID, name, tableID,
            columnID);
      this.putDescriptor(ConstraintDescriptor.class, primaryKey);
      this.putDescriptor(primaryKey);
      return constraintID;
   }

   /**
    * Drops a primary key from the system catalog.
    *
    * @param primaryKey
    *           primary key descriptor
    */
   private void dropPrimaryKey(final PrimaryKeyDescriptor primaryKey) {
      this.removeDescriptor(ConstraintDescriptor.class, primaryKey);
      this.removeDescriptor(primaryKey);
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKeyDescriptor(final int primaryKeyID) {
      return this.getDescriptor(PrimaryKeyDescriptor.class, primaryKeyID).orElseThrow(
            () -> new IllegalStateException("ID " + primaryKeyID + " does not reference a primary key."));
   }

   @Override
   public boolean hasPrimaryKey(final String name) {
      return this.getDescriptor(PrimaryKeyDescriptor.class, name).isPresent();
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKey(final String name) {
      return this.getDescriptor(PrimaryKeyDescriptor.class, name).orElseThrow(
            () -> new IllegalStateException("Primary key " + name + " does not exisit."));
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKey(final int tableID) {
      final Collection<PrimaryKeyDescriptor> primaryKeys = this.getDescriptors(PrimaryKeyDescriptor.class,
            tableID);
      return this.assertContainsOneOrNone(primaryKeys);
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKey(final int tableID, final int columnID) {
      final PrimaryKeyDescriptor primaryKey = this.getPrimaryKey(tableID);
      if (primaryKey.getColumnID() == columnID) {
         return primaryKey;
      }
      return null;
   }

   @Override
   public int createTableForeignKey(final String name, final int tableID, final int[] columnIDs,
         final int referenceTableID, final int[] referenceIDs) {
      // check if name is valid
      this.assertConstraintNotExists(name);
      // check if tableID is valid
      this.getTableDescriptor(tableID);
      // check if attributeIDs are valid
      for (final int id : columnIDs) {
         this.getColumnDescriptor(id);
      }
      // check if referenceIDs are valid
      for (final int id : referenceIDs) {
         this.getColumnDescriptor(id);
      }
      // everything is okay, create the new constraint
      final int constraintID = this.getNextID();
      final ForeignKeyDescriptor foreignKey = new ForeignKeyDescriptor(this, constraintID, name, tableID,
            columnIDs, referenceTableID, referenceIDs);
      this.putDescriptor(ConstraintDescriptor.class, foreignKey);
      this.putDescriptor(foreignKey);
      return constraintID;
   }

   @Override
   public int createColumnForeignKey(final String name, final int tableID, final int columnID,
         final int referenceTableID, final int referenceID) {
      // check if name is valid
      this.assertConstraintNotExists(name);
      // check if attributeID is valid
      this.getColumnDescriptor(columnID);
      // check if referenceID is valid
      this.getColumnDescriptor(referenceID);
      // everything is okay, create the new constraint
      final int constraintID = this.getNextID();
      final ConstraintDescriptor foreignKey = new ForeignKeyDescriptor(this, constraintID, name, tableID,
            columnID, referenceTableID, referenceID);
      this.putDescriptor(ConstraintDescriptor.class, foreignKey);
      this.putDescriptor(foreignKey);
      return constraintID;
   }

   /**
    * Drops a foreign key from the system catalog.
    *
    * @param foreignKey
    *           foreign key descriptor
    */
   private void dropForeignKey(final ForeignKeyDescriptor foreignKey) {
      this.removeDescriptor(ConstraintDescriptor.class, foreignKey);
      this.removeDescriptor(foreignKey);
   }

   @Override
   public ForeignKeyDescriptor getForeignKeyDescriptor(final int foreignKeyID) {
      return this.getDescriptor(ForeignKeyDescriptor.class, foreignKeyID).orElseThrow(
            () -> new IllegalStateException("ID " + foreignKeyID + " does not reference a foreign key."));
   }

   @Override
   public boolean hasForeignKey(final String name) {
      return this.getDescriptor(ForeignKeyDescriptor.class, name).isPresent();
   }

   @Override
   public ForeignKeyDescriptor getForeignKey(final String name) {
      return this.getDescriptor(ForeignKeyDescriptor.class, name).orElseThrow(
            () -> new IllegalStateException("Foreign key " + name + " does not exisit."));
   }

   @Override
   public Collection<ForeignKeyDescriptor> getForeignKeys(final int tableID) {
      return this.getDescriptors(ForeignKeyDescriptor.class, tableID);
   }

   @Override
   public Collection<ForeignKeyDescriptor> getForeignKeys(final int tableID, final int columnID) {
      return Collections.unmodifiableCollection(this.getForeignKeys(tableID).stream()
            .filter(fk -> fk.getColumnID() == columnID).collect(Collectors.<ForeignKeyDescriptor> toSet()));
   }

   @Override
   public void createIndexKey(final int ownerID, final int tableID, final int[] keyColumnIDs,
         final SortOrder[] orders) {
      final int indexKeyID = this.getNextID();
      final IndexKeyDescriptor indexKey = new IndexKeyDescriptor(this, indexKeyID, ownerID, keyColumnIDs,
            orders);
      this.putDescriptor(indexKey);
   }

   @Override
   public IndexKeyDescriptor getIndexKey(final int ownerID) {
      final Collection<IndexKeyDescriptor> indexKeys = this.getDescriptors(IndexKeyDescriptor.class, ownerID);
      return this.assertContainsOneOrNone(indexKeys);
   }

   /**
    * Drops an index key from the system catalog.
    *
    * @param indexKey
    *           index key descriptor
    */
   private void dropIndexKey(final IndexKeyDescriptor indexKey) {
      this.removeDescriptor(indexKey);
   }

   @Override
   public int createIndex(final IndexStatistics statistics, final String name, final IndexType type,
         final boolean clustered, final int[] keyColumnIDs, final SortOrder[] keyOrders, final int tableID) {
      this.assertTableContainsColumns(tableID, keyColumnIDs);
      final int indexID = this.getNextID();
      final IndexDescriptor index = new IndexDescriptor(this, statistics, indexID, name, type, clustered,
            keyColumnIDs, keyOrders, tableID);
      this.putDescriptor(index);
      return indexID;
   }

   @Override
   public void dropIndex(final int indexID) {
      final Optional<IndexDescriptor> index = this.getDescriptor(IndexDescriptor.class, indexID);
      if (index.isPresent()) {
         this.dropIndex(index.get());
      } else {
         throw new IllegalStateException("Index " + indexID + " does not exist.");
      }
   }

   @Override
   public void dropIndex(final String name) {
      final Optional<IndexDescriptor> index = this.getDescriptor(IndexDescriptor.class, name);
      if (index.isPresent()) {
         this.dropIndex(index.get());
      } else {
         throw new IllegalStateException("Index " + name + " does not exist.");
      }
   }

   /**
    * Drops an index and its index key from the system catalog.
    *
    * @param index
    *           index descriptor
    */
   private void dropIndex(final IndexDescriptor index) {
      final IndexKeyDescriptor indexKey = this.getIndexKey(index.getCatalogID());
      if (indexKey != null) {
         this.dropIndexKey(indexKey);
      }
      this.removeDescriptor(index);
   }

   @Override
   public IndexDescriptor getIndexDescriptor(final int indexID) {
      return this.getDescriptor(IndexDescriptor.class, indexID).orElseThrow(
            () -> new IllegalStateException("ID " + indexID + " does not reference an index."));
   }

   @Override
   public boolean hasIndex(final String name) {
      return this.getDescriptor(IndexDescriptor.class, name).isPresent();
   }

   @Override
   public IndexDescriptor getIndex(final String name) {
      return this.getDescriptor(IndexDescriptor.class, name).orElseThrow(
            () -> new IllegalStateException("Index " + name + " does not exist."));
   }

   @Override
   public Collection<IndexDescriptor> getIndexes(final int tableID) {
      return this.getDescriptors(IndexDescriptor.class);
   }

   @Override
   public int createBitmapIndex(final IndexStatistics statistics, final String name,
         final int[] keyColumnIDs, final int tableID) {
      this.assertTableContainsColumns(tableID, keyColumnIDs);
      final int indexID = this.getNextID();
      final BitmapIndexDescriptor index = new BitmapIndexDescriptor(this, statistics, indexID, name,
            keyColumnIDs, tableID);
      this.putDescriptor(index);
      return indexID;
   }

   @Override
   public int createBitmapJoinIndex(final IndexStatistics statistics, final String name,
         final int[] keyColumnIDs, final int[][] referencesIDs, final Expression predicate, final int tableID) {
      this.assertTableContainsColumns(tableID, keyColumnIDs);
      for (final int[] referenceIDs : referencesIDs) {
         this.assertSameTable(referenceIDs);
      }
      final int indexID = this.getNextID();
      final BitmapIndexDescriptor index = new BitmapIndexDescriptor(this, statistics, indexID, name,
            keyColumnIDs, referencesIDs, predicate, tableID);
      this.putDescriptor(index);
      return indexID;
   }

   /**
    * Drops a bitmap index and its index key from the system catalog.
    *
    * @param index
    *           bitmap index descriptor
    */
   private void dropBitmapIndex(final BitmapIndexDescriptor index) {
      final IndexKeyDescriptor indexKey = this.getIndexKey(index.getCatalogID());
      if (indexKey != null) {
         this.dropIndexKey(indexKey);
      }
      this.removeDescriptor(index);
   }

   @Override
   public BitmapIndexDescriptor getBitmapIndexDescriptor(final int indexID) {
      return this.getDescriptor(BitmapIndexDescriptor.class, indexID).orElseThrow(
            () -> new IllegalStateException("ID " + indexID + " does not reference a bitmap index."));
   }

   @Override
   public boolean hasBitmapIndex(final String name) {
      return this.getDescriptor(BitmapIndexDescriptor.class, name).isPresent();
   }

   @Override
   public BitmapIndexDescriptor getBitmapIndex(final String name) {
      return this.getDescriptor(BitmapIndexDescriptor.class, name).orElseThrow(
            () -> new IllegalStateException("Bitmap index " + name + " does not exist."));
   }

   @Override
   public Collection<BitmapIndexDescriptor> getBitmapIndexes(final int tableID) {
      return this.getDescriptors(BitmapIndexDescriptor.class);
   }

   /**
    * Asserts that the given column IDs are contained in the table given by its globally unique ID.
    *
    * @param tableID
    *           globally unique ID of the table
    * @param columnIDs
    *           globally unique IDs of the columns
    */
   private void assertTableContainsColumns(final int tableID, final int[] columnIDs) {
      for (final int columnID : columnIDs) {
         final ColumnDescriptor column = this.getColumnDescriptor(columnID);
         if (column.getOwnerID() != tableID) {
            throw new IllegalStateException("Column " + columnID + " not found in table " + tableID + ".");
         }
      }
   }

   /**
    * Asserts that the given column IDs are all contained in the same table.
    *
    * @param columnIDs
    *           globally unique IDs of the columns
    */
   private void assertSameTable(final int[] columnIDs) {
      int tableID = -1;
      for (final int columnID : columnIDs) {
         final ColumnDescriptor column = this.getColumnDescriptor(columnID);
         if (tableID == -1) {
            tableID = column.getOwnerID();
         } else if (tableID != column.getOwnerID()) {
            throw new IllegalStateException("Column " + columnID + " is not contained in table " + tableID
                  + ".");
         }
      }
   }
}

/*
 * @(#)PersistentSystemCatalog.java   1.0   Aug 23, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import minibase.RecordID;
import minibase.access.file.File;
import minibase.access.file.FileScan;
import minibase.access.file.HeapFile;
import minibase.catalog.SystemCatalogSchema.Column;
import minibase.catalog.SystemCatalogSchema.Table;
import minibase.query.optimizer.Expression;
import minibase.query.schema.Schema;
import minibase.storage.buffer.BufferManager;
import minibase.storage.file.DiskManager;

/**
 * Persistent implementation of the Minibase system catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public final class PersistentSystemCatalog extends AbstractSystemCatalog implements SystemCatalog {

   /** Maximum length of a tuple (in bytes). */
   public static final int MAX_TUPSIZE = DiskManager.PAGE_SIZE - 20;

   /** Maximum length of a column (in bytes). */
   public static final int MAX_COLSIZE = MAX_TUPSIZE - 3;

   /** Map of table catalog schema entries to heap files. */
   private final Map<Table, File> tables;

   /** Map of table catalog schema entries to schema definitions. */
   private final Map<Table, Schema> schemas;

   /** Map that caches the records of the tables table. */
   private final Map<Table, Record> tablesRecords;

   /** Map of identifier counters. */
   private final Counters counters;

   /**
    * Opens an existing persistent system catalog or creates a new one using the given buffer manager to
    * access the database.
    *
    * @param bufferManager
    *           the buffer manager used for managing the catalog tables
    * @param open
    *           {@code true} if the catalog exists and can be opened, {@code false} if it does not exist and
    *           should be created
    */
   private PersistentSystemCatalog(final BufferManager bufferManager, final boolean open) {
      this.tablesRecords = new HashMap<>();
      // Open or create the heap files for the catalog tables
      this.tables = new HashMap<>();
      this.tables.put(Table.TABLES, openOrCreate(bufferManager, Table.TABLES, open));
      this.tables.put(Table.COLUMNS, openOrCreate(bufferManager, Table.COLUMNS, open));
      this.tables.put(Table.CONSTRAINTS, openOrCreate(bufferManager, Table.CONSTRAINTS, open));
      this.tables.put(Table.PRIMARYKEYS, openOrCreate(bufferManager, Table.PRIMARYKEYS, open));
      this.tables.put(Table.FOREIGNKEYS, openOrCreate(bufferManager, Table.FOREIGNKEYS, open));
      this.tables.put(Table.INDEXES, openOrCreate(bufferManager, Table.INDEXES, open));
      this.tables.put(Table.INDEXKEYS, openOrCreate(bufferManager, Table.INDEXKEYS, open));
      // Initialize the schema mappings
      this.schemas = new HashMap<>();
      this.schemas.put(Table.TABLES, SystemCatalogSchema.TABLES_SCHEMA);
      this.schemas.put(Table.COLUMNS, SystemCatalogSchema.COLUMNS_SCHEMA);
      this.schemas.put(Table.CONSTRAINTS, SystemCatalogSchema.CONSTRAINTS_SCHEMA);
      this.schemas.put(Table.PRIMARYKEYS, SystemCatalogSchema.PRIMARYKEYS_SCHEMA);
      this.schemas.put(Table.FOREIGNKEYS, SystemCatalogSchema.FOREIGNKEYS_SCHEMA);
      this.schemas.put(Table.INDEXES, SystemCatalogSchema.INDEXES_SCHEMA);
      this.schemas.put(Table.INDEXKEYS, SystemCatalogSchema.INDEXKEYS_SCHEMA);
      // Initialize the identifier counters
      this.counters = this.initCounters();
      // Check if the system catalog needs bootstrapping
      if (this.counters.size() == 0) {
         this.bootstrap();
      }
   }

   /**
    * Opens or creates a heap file for the given system catalog table entry using the given buffer manager.
    *
    * @param bufferManager
    *           the buffer manager used for the heap file
    * @param table
    *           system catalog table entry
    * @param open
    *           {@code true} if the heap file exists and can be opened, {@code false} if it does not exist and
    *           should be created @return heap file used to manage the corresponding system catalog table
    */
   private static File openOrCreate(final BufferManager bufferManager, final Table table,
         final boolean open) {
      final String name = table.getName();
      if (open) {
         return HeapFile.open(bufferManager, name);
      }
      return HeapFile.create(bufferManager, name);
   }

   /**
    * Creates a new persistent system catalog using the given buffer manager.
    *
    * @param bufferManager
    *           buffer manager to use
    * @return persistent system catalog
    */
   public static SystemCatalog create(final BufferManager bufferManager) {
      return new PersistentSystemCatalog(bufferManager, false);
   }

   /**
    * Opens an existing system catalog using the given buffer manager.
    *
    * @param bufferManager
    *           buffer manager to use
    * @return persistent system catalog
    */
   public static SystemCatalog open(final BufferManager bufferManager) {
      return new PersistentSystemCatalog(bufferManager, true);
   }

   @Override
   public int getPageSize() {
      return DiskManager.PAGE_SIZE;
   }

   @Override
   public int createTable(final TableStatistics statistics, final String name, final DataOrder order) {
      if (this.getTableRecord(name).isPresent()) {
         throw new IllegalStateException("Table " + name + " exists already.");
      }
      final int tableID = this.counters.getNextID(Table.TABLES.getName());
      final byte[] tuple = SystemCatalogSchema.TABLES_SCHEMA.newTuple();
      SystemCatalogSchema.TABLES_SCHEMA.setAllFields(tuple, tableID, name, order.ordinal(),
            (int) statistics.getCardinality(), 0);
      this.tables.get(Table.TABLES).insertRecord(tuple);
      this.updateCounter(Table.TABLES, this.getTableRecord(Table.TABLES));
      return tableID;
   }

   @Override
   public int createTable(final TableStatistics statistics, final String name) {
      return this.createTable(statistics, name, DataOrder.ANY);
   }

   @Override
   public void dropTable(final int tableID) {
      final Optional<Record> record = this.getTableRecord(tableID);
      if (record.isPresent()) {
         this.dropTable(record.get());
      } else {
         throw new IllegalStateException("Table " + tableID + " does not exist.");
      }
   }

   @Override
   public void dropTable(final String name) {
      final Optional<Record> record = this.getTableRecord(name);
      if (record.isPresent()) {
         this.dropTable(record.get());
      } else {
         throw new IllegalStateException("Table " + name + " does not exist.");
      }
   }

   /**
    * Drops a table, its columns, its indexes, and its constraints from the system catalog.
    *
    * @param table
    *           table record
    */
   private void dropTable(final Record table) {
      final int tableID = SystemCatalogSchema.TABLES_SCHEMA.getIntField(table.getValue(),
            Column.TABLES_TABID.getPosition());
      // Drop table columns
      for (final Record column : this.getColumnRecords(tableID)) {
         this.dropColumn(column);
      }
      // Drop table indexes
      for (final Record index : this.getIndexRecords(tableID)) {
         this.dropIndex(index);
      }
      // TODO Drop table bitmap indexes
      // Drop table index key
      for (final Record indexKey : this.getIndexKeyRecords(tableID)) {
         this.dropIndexKey(indexKey);
      }
      // Drop table primary key
      for (final Record primaryKey : this.getConstraintRecords(tableID, ConstraintType.PRIMARY_KEY)) {
         this.dropPrimaryKey(primaryKey);
      }
      // Drop table foreign keys
      for (final Record foreignKey : this.getConstraintRecords(tableID, ConstraintType.FOREIGN_KEY)) {
         this.dropForeignKey(foreignKey);
      }
      // Drop table
      final Optional<TableDescriptor> descriptor = this.getDescriptor(TableDescriptor.class, tableID);
      if (descriptor.isPresent()) {
         this.removeDescriptor(descriptor.get());
      }
      this.tables.get(Table.TABLES).deleteRecord(table.getRecordID());
   }

   @Override
   public TableDescriptor getTableDescriptor(final int tableID) {
      final Optional<TableDescriptor> table = this.getDescriptor(TableDescriptor.class, tableID);
      if (table.isPresent()) {
         return table.get();
      }
      return this.createTableDescriptor(this.getTableRecord(tableID).map(Record::getValue)
            .orElseThrow(() -> new IllegalStateException("ID " + tableID + " does not reference a table.")));
   }

   @Override
   public boolean hasTable(final String name) {
      return this.getDescriptor(TableDescriptor.class, name).isPresent()
            || this.getTableRecord(name).isPresent();
   }

   @Override
   public TableDescriptor getTable(final String name) {
      final Optional<TableDescriptor> table = this.getDescriptor(TableDescriptor.class, name);
      if (table.isPresent()) {
         return table.get();
      }
      return this.createTableDescriptor(this.getTableRecord(name).map(Record::getValue)
            .orElseThrow(() -> new IllegalStateException("Table " + name + " does not exist.")));
   }

   @Override
   public Collection<TableDescriptor> getTables() {
      final List<Record> records = this.getRecords(Table.TABLES, new Column[0], new Object[0]);
      final Set<TableDescriptor> tables = new HashSet<>();
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int tableID = SystemCatalogSchema.TABLES_SCHEMA.getIntField(tuple,
               Column.TABLES_TABID.getPosition());
         final Optional<TableDescriptor> table = this.getDescriptor(TableDescriptor.class, tableID);
         if (table.isPresent()) {
            tables.add(table.get());
         } else {
            tables.add(this.createTableDescriptor(tuple));
         }
      }
      return Collections.unmodifiableCollection(tables);
   }

   /**
    * Creates and caches a table descriptor from the given table tuple.
    *
    * @param tuple
    *           table tuple
    * @return table descriptor
    */
   private TableDescriptor createTableDescriptor(final byte[] tuple) {
      final int tableID = SystemCatalogSchema.TABLES_SCHEMA.getIntField(tuple,
            Column.TABLES_TABID.getPosition());
      final String name = SystemCatalogSchema.TABLES_SCHEMA.getCharField(tuple,
            Column.TABLES_NAME.getPosition());
      final int order = SystemCatalogSchema.TABLES_SCHEMA.getIntField(tuple,
            Column.TABLES_ORDER.getPosition());
      final int cardinality = SystemCatalogSchema.TABLES_SCHEMA.getIntField(tuple,
            Column.TABLES_CARD.getPosition());
      // TODO Calculate size
      final int size = 0;
      final TableStatistics statistics = new TableStatistics(cardinality, cardinality,
            size / DiskManager.PAGE_SIZE);
      final TableDescriptor descriptor = new TableDescriptor(this, tableID, name, DataOrder.values()[order],
            size, statistics);
      this.putDescriptor(descriptor);
      return descriptor;
   }

   @Override
   public int createColumn(final ColumnStatistics statistics, final String name, final DataType type,
         final int size, final int tableID) {
      if (this.getColumnRecord(name, tableID).isPresent()) {
         throw new IllegalStateException("Column " + name + " exists already.");
      }
      final int actualSize;
      if (type.getSize() < 0) {
         if (size > 0) {
            actualSize = size;
         } else {
            throw new IllegalArgumentException("The size of an column must be greater or equal to 1.");
         }
      } else {
         actualSize = type.getSize();
      }
      final Object minObject = statistics.getMinimum();
      final Object maxObject = statistics.getMaximum();
      final float minimum = minObject == null ? Float.NaN : ((Number) minObject).floatValue();
      final float maximum = maxObject == null ? Float.NaN : ((Number) maxObject).floatValue();
      final int columnID = this.counters.getNextID(Table.COLUMNS.getName());
      final byte[] tuple = SystemCatalogSchema.COLUMNS_SCHEMA.newTuple();
      // TODO Set column position
      SystemCatalogSchema.COLUMNS_SCHEMA.setAllFields(tuple, columnID, tableID, -1, name, type.ordinal(),
            actualSize, (int) statistics.getCardinality(), (int) statistics.getUniqueCardinality(), minimum,
            maximum);
      this.tables.get(Table.COLUMNS).insertRecord(tuple);
      this.updateCounter(Table.COLUMNS, this.getTableRecord(Table.COLUMNS));
      return columnID;
   }

   /**
    * Drops a column from the system catalog.
    *
    * @param column
    *           column tuple
    */
   private void dropColumn(final Record column) {
      final int tableID = SystemCatalogSchema.COLUMNS_SCHEMA.getIntField(column.getValue(),
            Column.COLUMNS_TABID.getPosition());
      final int columnID = SystemCatalogSchema.COLUMNS_SCHEMA.getIntField(column.getValue(),
            Column.COLUMNS_COLID.getPosition());
      // Drop column primary key
      for (final Record primaryKey : this.getConstraintRecords(tableID, columnID,
            ConstraintType.PRIMARY_KEY)) {
         this.dropPrimaryKey(primaryKey);
      }
      // Drop column foreign keys
      for (final Record foreignKey : this.getConstraintRecords(tableID, columnID,
            ConstraintType.FOREIGN_KEY)) {
         this.dropForeignKey(foreignKey);
      }
      // Drop column
      final Optional<ColumnDescriptor> descriptor = this.getDescriptor(ColumnDescriptor.class, columnID);
      if (descriptor.isPresent()) {
         this.removeDescriptor(descriptor.get());
      }
      this.tables.get(Table.COLUMNS).deleteRecord(column.getRecordID());
   }

   @Override
   public ColumnDescriptor getColumnDescriptor(final int columnID) {
      final Optional<ColumnDescriptor> column = this.getDescriptor(ColumnDescriptor.class, columnID);
      if (column.isPresent()) {
         return column.get();
      }
      final Record record = this.getColumnRecord(columnID)
            .orElseThrow(() -> new IllegalStateException("ID " + columnID + " does not reference a column."));
      return this.createColumnDescriptor(record.getValue());
   }

   @Override
   public boolean hasColumn(final int tableID, final String name) {
      return this.getDescriptor(ColumnDescriptor.class, tableID, name).isPresent()
            || this.getColumnRecord(name, tableID).isPresent();
   }

   @Override
   public ColumnDescriptor getColumn(final int tableID, final String name) {
      final Optional<ColumnDescriptor> column = this.getDescriptor(ColumnDescriptor.class, name);
      if (column.isPresent()) {
         return column.get();
      }
      final Record record = this.getColumnRecord(name, tableID)
            .orElseThrow(() -> new IllegalStateException("Column " + name + " does not exist."));
      return this.createColumnDescriptor(record.getValue());
   }

   @Override
   public List<ColumnDescriptor> getColumns(final int tableID) {
      final List<Record> records = this.getColumnRecords(tableID);
      final List<ColumnDescriptor> columns = new ArrayList<>();
      final Schema schema = SystemCatalogSchema.COLUMNS_SCHEMA;
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int columnID = schema.getIntField(tuple, Column.COLUMNS_COLID.getPosition());
         final Optional<ColumnDescriptor> column = this.getDescriptor(ColumnDescriptor.class, columnID);
         if (column.isPresent()) {
            columns.add(column.get());
         } else {
            columns.add(this.createColumnDescriptor(tuple));
         }
      }
      // TODO Sort columns according to position!
      return Collections.unmodifiableList(columns);
   }

   /**
    * Creates and caches a column descriptor from the given column tuple.
    *
    * @param tuple
    *           column tuple
    * @return column descriptor
    */
   private ColumnDescriptor createColumnDescriptor(final byte[] tuple) {
      final Schema schema = SystemCatalogSchema.COLUMNS_SCHEMA;
      final int columnID = schema.getIntField(tuple, Column.COLUMNS_COLID.getPosition());
      final String name = schema.getCharField(tuple, Column.COLUMNS_NAME.getPosition());
      final int tableID = schema.getIntField(tuple, Column.COLUMNS_TABID.getPosition());
      final DataType type = DataType.values()[schema.getIntField(tuple, Column.COLUMNS_TYPE.getPosition())];
      final int size = schema.getIntField(tuple, Column.COLUMNS_SIZE.getPosition());
      final int cardinality = schema.getIntField(tuple, Column.COLUMNS_CARD.getPosition());
      final int uniqueCardinality = schema.getIntField(tuple, Column.COLUMNS_UCARD.getPosition());
      final float minimum = schema.getFloatField(tuple, Column.COLUMNS_MIN.getPosition());
      final float maximum = schema.getFloatField(tuple, Column.COLUMNS_MAX.getPosition());
      final ColumnStatistics statistics = new ColumnStatistics(cardinality, uniqueCardinality, minimum,
            maximum, (double) size / DiskManager.PAGE_SIZE);
      final ColumnDescriptor descriptor = new ColumnDescriptor(this, columnID, name, type, size, tableID,
            statistics);
      this.putDescriptor(descriptor);
      return descriptor;
   }

   /**
    * Creates a constraint with the given name and type for the table and column with the given identifiers.
    *
    * @param name
    *           constraint identifier
    * @param tableID
    *           table identifier
    * @param columnID
    *           column identifier
    * @param type
    *           constraint type
    * @return identifier of the new constraint
    */
   private int createConstraint(final String name, final int tableID, final int columnID,
         final ConstraintType type) {
      if (this.getConstraintRecord(name).isPresent()) {
         throw new IllegalStateException("Constraint " + name + " already exists.");
      }
      final int constraintID = this.counters.getNextID(Table.CONSTRAINTS.getName());
      final Schema schema = SystemCatalogSchema.CONSTRAINTS_SCHEMA;
      final byte[] tuple = schema.newTuple();
      schema.setAllFields(tuple, constraintID, tableID, columnID, name, type.ordinal());
      this.tables.get(Table.CONSTRAINTS).insertRecord(tuple);
      return constraintID;
   }

   @Override
   public ConstraintDescriptor getConstraintDescriptor(final int constraintID) {
      final Optional<ConstraintDescriptor> constraint = this.getDescriptor(ConstraintDescriptor.class,
            constraintID);
      if (constraint.isPresent()) {
         return constraint.get();
      }
      final Record record = this.getConstraintRecord(constraintID).orElseThrow(
            () -> new IllegalStateException("ID " + constraintID + " does not reference a constraint."));
      return this.createConstraintDescriptor(record.getValue());
   }

   @Override
   public boolean hasConstraint(final String name) {
      return this.getDescriptor(ConstraintDescriptor.class, name).isPresent()
            || this.getConstraintRecord(name).isPresent();
   }

   @Override
   public ConstraintDescriptor getConstraint(final String name) {
      final Optional<ConstraintDescriptor> constraint = this.getDescriptor(ConstraintDescriptor.class, name);
      if (constraint.isPresent()) {
         return constraint.get();
      }
      final Record record = this.getConstraintRecord(name)
            .orElseThrow(() -> new IllegalStateException("Constraint " + name + " does not exist."));
      return this.createConstraintDescriptor(record.getValue());
   }

   @Override
   public Collection<ConstraintDescriptor> getConstraints(final int tableID) {
      final Schema schema = SystemCatalogSchema.CONSTRAINTS_SCHEMA;
      final List<Record> records = this.getRecords(Table.CONSTRAINTS,
            new Column[] { Column.CONSTRAINTS_TABID }, new Object[] { tableID });
      final Collection<ConstraintDescriptor> result = new HashSet<>();
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int constraintID = schema.getIntField(tuple, Column.CONSTRAINTS_CONSTID.getPosition());
         final Optional<ConstraintDescriptor> constraint = this.getDescriptor(ConstraintDescriptor.class,
               constraintID);
         if (constraint.isPresent()) {
            result.add(constraint.get());
         } else {
            result.add(this.createConstraintDescriptor(tuple));
         }
      }
      return Collections.unmodifiableCollection(result);
   }

   /**
    * Creates and caches a constraint descriptor from the given constraint tuple.
    *
    * @param tuple
    *           constraint tuple
    * @return constraint descriptor
    */
   private ConstraintDescriptor createConstraintDescriptor(final byte[] tuple) {
      final Schema schema = SystemCatalogSchema.CONSTRAINTS_SCHEMA;
      final ConstraintType type = ConstraintType.values()[schema.getIntField(tuple,
            Column.CONSTRAINTS_TYPE.getPosition())];
      final ConstraintDescriptor descriptor;
      switch (type) {
         case PRIMARY_KEY:
            descriptor = this.createPrimaryKeyDescriptor(tuple);
            break;
         case FOREIGN_KEY:
            descriptor = this.createForeignKeyDescriptor(tuple);
            break;
         default:
            throw new IllegalStateException("Constraint type " + type + " is not supported.");
      }
      this.putDescriptor(ConstraintDescriptor.class, descriptor);
      return descriptor;
   }

   @Override
   public int createTablePrimaryKey(final String name, final int tableID, final int[] columnIDs) {
      return this.createPrimaryKey(name, tableID, INVALID_ID, columnIDs);
   }

   @Override
   public int createColumnPrimaryKey(final String name, final int tableID, final int columnID) {
      return this.createPrimaryKey(name, tableID, columnID, new int[] { columnID });
   }

   /**
    * Creates a new primary key with the given name on the given table that consists of the given columns.
    *
    * @param name
    *           primary key name
    * @param tableID
    *           table with the primary key
    * @param columnID
    *           column with the primary key, {@link SystemCatalog#INVALID_ID} if the primary key is a table
    *           constraint
    * @param columnIDs
    *           columns of the primary key
    * @return identifier of the new primary key
    */
   private int createPrimaryKey(final String name, final int tableID, final int columnID,
         final int[] columnIDs) {
      final int pkeyID = this.createConstraint(name, tableID, columnID, ConstraintType.PRIMARY_KEY);
      final byte[] tuple = SystemCatalogSchema.PRIMARYKEYS_SCHEMA.newTuple();
      for (int i = 0; i < columnIDs.length; i++) {
         SystemCatalogSchema.PRIMARYKEYS_SCHEMA.setAllFields(tuple, pkeyID, tableID, columnIDs[i], i);
         this.tables.get(Table.PRIMARYKEYS).insertRecord(tuple);
      }
      this.updateCounter(Table.CONSTRAINTS, this.getTableRecord(Table.CONSTRAINTS));
      return pkeyID;
   }

   /**
    * Drops a primary key from the system catalog.
    *
    * @param primaryKey
    *           primary key record
    */
   private void dropPrimaryKey(final Record primaryKey) {
      final Schema schema = SystemCatalogSchema.CONSTRAINTS_SCHEMA;
      final int primaryKeyID = schema.getIntField(primaryKey.getValue(),
            Column.CONSTRAINTS_CONSTID.getPosition());
      // Drop primary key records
      final List<Record> records = this.getRecords(Table.PRIMARYKEYS,
            new Column[] { Column.PRIMARYKEYS_CONSTID }, new Object[] { primaryKeyID });
      final File file = this.tables.get(Table.PRIMARYKEYS);
      for (final Record record : records) {
         file.deleteRecord(record.getRecordID());
      }
      // Drop primary key constraint record
      final Optional<PrimaryKeyDescriptor> descriptor = this.getDescriptor(PrimaryKeyDescriptor.class,
            primaryKeyID);
      if (descriptor.isPresent()) {
         this.removeDescriptor(descriptor.get());
      }
      this.tables.get(Table.CONSTRAINTS).deleteRecord(primaryKey.getRecordID());
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKeyDescriptor(final int primaryKeyID) {
      final Optional<PrimaryKeyDescriptor> key = this.getDescriptor(PrimaryKeyDescriptor.class, primaryKeyID);
      if (key.isPresent()) {
         return key.get();
      }
      final Record record = this.getConstraintRecord(primaryKeyID, ConstraintType.PRIMARY_KEY).orElseThrow(
            () -> new IllegalStateException("ID " + primaryKeyID + " does not reference a primary key."));
      return this.createPrimaryKeyDescriptor(record.getValue());
   }

   @Override
   public boolean hasPrimaryKey(final String name) {
      return this.getDescriptor(PrimaryKeyDescriptor.class, name).isPresent()
            || this.getConstraintRecord(name, ConstraintType.PRIMARY_KEY).isPresent();
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKey(final String name) {
      final Optional<PrimaryKeyDescriptor> key = this.getDescriptor(PrimaryKeyDescriptor.class, name);
      if (key.isPresent()) {
         return key.get();
      }
      final Record record = this.getConstraintRecord(name, ConstraintType.PRIMARY_KEY)
            .orElseThrow(() -> new IllegalStateException("Primary key " + name + " does not exisit."));
      return this.createPrimaryKeyDescriptor(record.getValue());
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKey(final int tableID) {
      final List<PrimaryKeyDescriptor> keys = this.getDescriptors(PrimaryKeyDescriptor.class, tableID);
      if (!keys.isEmpty()) {
         return keys.get(0);
      }
      final List<Record> records = this.getConstraintRecords(tableID, ConstraintType.PRIMARY_KEY);
      final Record record = this.assertContainsOneOrNone(records);
      if (record != null) {
         return this.createPrimaryKeyDescriptor(record.getValue());
      }
      return null;
   }

   @Override
   public PrimaryKeyDescriptor getPrimaryKey(final int tableID, final int columnID) {
      final List<PrimaryKeyDescriptor> keys = this.getDescriptors(PrimaryKeyDescriptor.class, tableID);
      if (!keys.isEmpty()) {
         final PrimaryKeyDescriptor key = keys.get(0);
         if (key.getColumnID() == columnID) {
            return key;
         }
      }
      final List<Record> records = this.getConstraintRecords(tableID, columnID, ConstraintType.PRIMARY_KEY);
      final Record record = this.assertContainsOneOrNone(records);
      if (record != null) {
         return this.createPrimaryKeyDescriptor(record.getValue());
      }
      return null;
   }

   /**
    * Creates and caches a primary key descriptor from the given constraint tuple.
    *
    * @param tuple
    *           constraint tuple
    * @return primary key descriptor
    */
   private PrimaryKeyDescriptor createPrimaryKeyDescriptor(final byte[] tuple) {
      final Schema constraintsSchema = SystemCatalogSchema.CONSTRAINTS_SCHEMA;
      final int constraintID = constraintsSchema.getIntField(tuple, Column.CONSTRAINTS_CONSTID.getPosition());
      final String name = constraintsSchema.getCharField(tuple, Column.CONSTRAINTS_NAME.getPosition());
      final int ownerID = constraintsSchema.getIntField(tuple, Column.CONSTRAINTS_TABID.getPosition());
      int columnID = constraintsSchema.getIntField(tuple, Column.CONSTRAINTS_COLID.getPosition());
      final PrimaryKeyDescriptor descriptor;
      if (columnID == INVALID_ID) {
         // table constraint
         final Schema primaryKeysSchema = SystemCatalogSchema.PRIMARYKEYS_SCHEMA;
         final List<Record> records = this.getRecords(Table.PRIMARYKEYS,
               new Column[] { Column.PRIMARYKEYS_CONSTID }, new Object[] { constraintID });
         final int[] columnIDs = new int[records.size()];
         for (final Record r : records) {
            final byte[] t = r.getValue();
            columnID = primaryKeysSchema.getIntField(t, Column.PRIMARYKEYS_COLID.getPosition());
            final int keyNo = primaryKeysSchema.getIntField(t, Column.PRIMARYKEYS_KEYNO.getPosition());
            columnIDs[keyNo] = columnID;
         }
         descriptor = new PrimaryKeyDescriptor(this, constraintID, name, ownerID, columnIDs);
      } else {
         // column constraint
         descriptor = new PrimaryKeyDescriptor(this, constraintID, name, ownerID, columnID);
      }
      this.putDescriptor(descriptor);
      return descriptor;
   }

   @Override
   public int createTableForeignKey(final String name, final int tableID, final int[] columnIDs,
         final int referenceTableID, final int[] referenceIDs) {
      return this.createForeignKey(name, tableID, INVALID_ID, columnIDs, referenceTableID, referenceIDs);
   }

   @Override
   public int createColumnForeignKey(final String name, final int tableID, final int columnID,
         final int referenceTableID, final int referenceID) {
      return this.createForeignKey(name, tableID, columnID, new int[] { columnID }, referenceTableID,
            new int[] { referenceID });
   }

   /**
    * Creates a new foreign key with the given name on the given table that consists of the given referencing
    * and referenced columns.
    *
    * @param name
    *           foreign key name
    * @param tableID
    *           table with the foreign key
    * @param columnID
    *           column with the foreign key, {@link SystemCatalog#INVALID_ID} if the foreign key is a table
    *           constraint
    * @param columnIDs
    *           referencing columns of the foreign key
    * @param referenceTableID
    *           table that contains the referenced tables
    * @param referenceIDs
    *           reference columns of the foreign key
    * @return identifier of the new foreign key
    */
   private int createForeignKey(final String name, final int tableID, final int columnID,
         final int[] columnIDs, final int referenceTableID, final int[] referenceIDs) {
      if (columnIDs.length != referenceIDs.length) {
         throw new IllegalArgumentException("Number of referencing and referenced columns does not match.");
      }
      final Schema schema = SystemCatalogSchema.FOREIGNKEYS_SCHEMA;
      final int fkeyID = this.createConstraint(name, tableID, columnID, ConstraintType.FOREIGN_KEY);
      final byte[] tuple = schema.newTuple();
      for (int i = 0; i < columnIDs.length; i++) {
         schema.setAllFields(tuple, fkeyID, tableID, referenceTableID, columnIDs[i], referenceIDs[i], i);
         this.tables.get(Table.FOREIGNKEYS).insertRecord(tuple);
      }
      this.updateCounter(Table.CONSTRAINTS, this.getTableRecord(Table.CONSTRAINTS));
      return fkeyID;
   }

   /**
    * Drops the foreign key represented by the given record from the system catalog.
    *
    * @param foreignKey
    *           foreign key record
    */

   private void dropForeignKey(final Record foreignKey) {
      final Schema schema = SystemCatalogSchema.FOREIGNKEYS_SCHEMA;
      final int foreignKeyID = schema.getIntField(foreignKey.getValue(),
            Column.FOREIGNKEYS_CONSTID.getPosition());
      // Drop foreign key records
      final List<Record> records = this.getRecords(Table.FOREIGNKEYS,
            new Column[] { Column.FOREIGNKEYS_CONSTID }, new Object[] { foreignKeyID });
      final File file = this.tables.get(Table.FOREIGNKEYS);
      for (final Record record : records) {
         file.deleteRecord(record.getRecordID());
      }
      // Drop foreign key constraint record
      final Optional<ForeignKeyDescriptor> descriptor = this.getDescriptor(ForeignKeyDescriptor.class,
            foreignKeyID);
      if (descriptor.isPresent()) {
         this.removeDescriptor(descriptor.get());
      }
      this.tables.get(Table.CONSTRAINTS).deleteRecord(foreignKey.getRecordID());
   }

   @Override
   public ForeignKeyDescriptor getForeignKeyDescriptor(final int foreignKeyID) {
      final Optional<ForeignKeyDescriptor> key = this.getDescriptor(ForeignKeyDescriptor.class, foreignKeyID);
      if (key.isPresent()) {
         return key.get();
      }
      final Record record = this.getConstraintRecord(foreignKeyID, ConstraintType.FOREIGN_KEY).orElseThrow(
            () -> new IllegalStateException("ID " + foreignKeyID + " does not reference a foreign key."));
      return this.createForeignKeyDescriptor(record.getValue());
   }

   @Override
   public boolean hasForeignKey(final String name) {
      return this.getDescriptor(ForeignKeyDescriptor.class, name).isPresent()
            || this.getConstraintRecord(name, ConstraintType.FOREIGN_KEY).isPresent();
   }

   @Override
   public ForeignKeyDescriptor getForeignKey(final String name) {
      final Optional<ForeignKeyDescriptor> key = this.getDescriptor(ForeignKeyDescriptor.class, name);
      if (key.isPresent()) {
         return key.get();
      }
      final Record record = this.getConstraintRecord(name, ConstraintType.FOREIGN_KEY)
            .orElseThrow(() -> new IllegalStateException("Foreign key " + name + " does not exisit."));
      return this.createForeignKeyDescriptor(record.getValue());
   }

   @Override
   public Collection<ForeignKeyDescriptor> getForeignKeys(final int tableID) {
      final List<Record> records = this.getConstraintRecords(tableID, ConstraintType.FOREIGN_KEY);
      final Collection<ForeignKeyDescriptor> result = new HashSet<>();
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int constraintID = SystemCatalogSchema.CONSTRAINTS_SCHEMA.getIntField(tuple,
               Column.CONSTRAINTS_CONSTID.getPosition());
         final Optional<ForeignKeyDescriptor> foreignKey = this.getDescriptor(ForeignKeyDescriptor.class,
               constraintID);
         if (foreignKey.isPresent()) {
            result.add(foreignKey.get());
         } else {
            result.add(this.createForeignKeyDescriptor(tuple));
         }
      }
      return Collections.unmodifiableCollection(result);
   }

   @Override
   public Collection<ForeignKeyDescriptor> getForeignKeys(final int tableID, final int columnID) {
      final List<Record> records = this.getConstraintRecords(tableID, columnID, ConstraintType.FOREIGN_KEY);
      final Collection<ForeignKeyDescriptor> result = new HashSet<>();
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int constraintID = SystemCatalogSchema.CONSTRAINTS_SCHEMA.getIntField(tuple,
               Column.CONSTRAINTS_CONSTID.getPosition());
         final Optional<ForeignKeyDescriptor> foreignKey = this.getDescriptor(ForeignKeyDescriptor.class,
               constraintID);
         if (foreignKey.isPresent()) {
            result.add(foreignKey.get());
         } else {
            result.add(this.createForeignKeyDescriptor(tuple));
         }
      }
      return Collections.unmodifiableCollection(result);
   }

   /**
    * Creates and caches a foreign key descriptor from the given constraint tuple.
    *
    * @param tuple
    *           constraint tuple
    * @return foreign key descriptor
    */
   private ForeignKeyDescriptor createForeignKeyDescriptor(final byte[] tuple) {
      final Schema constraintsSchema = SystemCatalogSchema.CONSTRAINTS_SCHEMA;
      final Schema foreignKeysSchema = SystemCatalogSchema.FOREIGNKEYS_SCHEMA;
      final int constraintID = constraintsSchema.getIntField(tuple, Column.CONSTRAINTS_CONSTID.getPosition());
      final String name = constraintsSchema.getCharField(tuple, Column.CONSTRAINTS_NAME.getPosition());
      final int ownerID = constraintsSchema.getIntField(tuple, Column.CONSTRAINTS_TABID.getPosition());
      final int columnID = constraintsSchema.getIntField(tuple, Column.CONSTRAINTS_COLID.getPosition());
      final ForeignKeyDescriptor descriptor;
      if (columnID == INVALID_ID) {
         // table constraint
         final List<Record> records = this.getRecords(Table.FOREIGNKEYS,
               new Column[] { Column.FOREIGNKEYS_CONSTID }, new Object[] { constraintID });
         final int[] columnIDs = new int[records.size()];
         final int[] referencedIDs = new int[records.size()];
         int referencedTableID = INVALID_ID;
         for (final Record r : records) {
            final byte[] t = r.getValue();
            referencedTableID = foreignKeysSchema.getIntField(t, Column.FOREIGNKEYS_RKEYID.getPosition());
            final int keyNo = foreignKeysSchema.getIntField(t, Column.FOREIGNKEYS_KEYNO.getPosition());
            columnIDs[keyNo] = foreignKeysSchema.getIntField(t, Column.FOREIGNKEYS_FKEY.getPosition());
            referencedIDs[keyNo] = foreignKeysSchema.getIntField(t, Column.FOREIGNKEYS_RKEY.getPosition());
         }
         descriptor = new ForeignKeyDescriptor(this, constraintID, name, ownerID, columnIDs,
               referencedTableID, referencedIDs);
      } else {
         // column constraint
         final Record r = this.getRecord(Table.FOREIGNKEYS, new Column[] { Column.FOREIGNKEYS_CONSTID },
               new Object[] { constraintID }).get();
         final byte[] t = r.getValue();
         final int referencedTableID = foreignKeysSchema.getIntField(t,
               Column.FOREIGNKEYS_RKEYID.getPosition());
         final int referencedID = foreignKeysSchema.getIntField(t, Column.FOREIGNKEYS_RKEY.getPosition());
         descriptor = new ForeignKeyDescriptor(this, constraintID, name, ownerID, columnID, referencedTableID,
               referencedID);
      }
      this.putDescriptor(descriptor);
      return descriptor;
   }

   @Override
   public int createIndex(final IndexStatistics statistics, final String name, final IndexType type,
         final boolean clustered, final int[] keyColumnIDs, final SortOrder[] keyOrders, final int tableID) {
      if (this.getIndexRecord(name).isPresent()) {
         throw new IllegalStateException("Index " + name + " exists already.");
      }
      final int indexID = this.counters.getNextID(Table.INDEXES.getName());
      final byte[] tuple = SystemCatalogSchema.INDEXES_SCHEMA.newTuple();
      SystemCatalogSchema.INDEXES_SCHEMA.setAllFields(tuple, indexID, tableID, name, type.ordinal(),
            clustered ? 1 : 0);
      this.tables.get(Table.INDEXES).insertRecord(tuple);
      this.updateCounter(Table.INDEXES, this.getTableRecord(Table.INDEXES));
      this.createIndexKey(indexID, tableID, keyColumnIDs, keyOrders);
      return indexID;
   }

   @Override
   public void dropIndex(final int indexID) {
      final Optional<Record> record = this.getIndexRecord(indexID);
      if (record.isPresent()) {
         this.dropIndex(record.get());
      } else {
         throw new IllegalStateException("Index " + indexID + " does not exist.");
      }
   }

   @Override
   public void dropIndex(final String name) {
      final Optional<Record> record = this.getIndexRecord(name);
      if (record.isPresent()) {
         this.dropIndex(record.get());
      } else {
         throw new IllegalStateException("Index " + name + " does not exist.");
      }
   }

   /**
    * Drops the index represented by the given record from the system catalog.
    *
    * @param index
    *           index record
    */
   private void dropIndex(final Record index) {
      final int indexID = SystemCatalogSchema.INDEXES_SCHEMA.getIntField(index.getValue(),
            Column.INDEXES_IDXID.getPosition());
      // Drop index key records
      for (final Record indexKey : this.getIndexKeyRecords(indexID)) {
         this.dropIndexKey(indexKey);
      }
      // Drop index record
      final Optional<IndexDescriptor> descriptor = this.getDescriptor(IndexDescriptor.class, indexID);
      if (descriptor.isPresent()) {
         this.removeDescriptor(descriptor.get());
      }
      this.tables.get(Table.INDEXES).deleteRecord(index.getRecordID());
   }

   @Override
   public IndexDescriptor getIndexDescriptor(final int indexID) {
      final Optional<IndexDescriptor> index = this.getDescriptor(IndexDescriptor.class, indexID);
      if (index.isPresent()) {
         return index.get();
      }
      final Record record = this.getIndexRecord(indexID)
            .orElseThrow(() -> new IllegalStateException("ID " + indexID + " does not reference an index."));
      return this.createIndexDescriptor(record.getValue());
   }

   @Override
   public boolean hasIndex(final String name) {
      return this.getDescriptor(IndexDescriptor.class, name).isPresent()
            || this.getIndexRecord(name).isPresent();
   }

   @Override
   public IndexDescriptor getIndex(final String name) {
      final Optional<IndexDescriptor> index = this.getDescriptor(IndexDescriptor.class, name);
      if (index.isPresent()) {
         return index.get();
      }
      final Record record = this.getIndexRecord(name)
            .orElseThrow(() -> new IllegalStateException("Index " + name + " does not exist."));
      return this.createIndexDescriptor(record.getValue());
   }

   @Override
   public Collection<IndexDescriptor> getIndexes(final int tableID) {
      final List<Record> records = this.getIndexRecords(tableID);
      final Set<IndexDescriptor> indexes = new HashSet<>();
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int indexID = SystemCatalogSchema.INDEXES_SCHEMA.getIntField(tuple,
               Column.INDEXES_IDXID.getPosition());
         final Optional<IndexDescriptor> index = this.getDescriptor(IndexDescriptor.class, indexID);
         if (index.isPresent()) {
            indexes.add(index.get());
         } else {
            indexes.add(this.createIndexDescriptor(tuple));
         }
      }
      return Collections.unmodifiableCollection(indexes);
   }

   /**
    * Creates and caches an index descriptor from the given index tuple.
    *
    * @param tuple
    *           index tuple
    * @return index descriptor
    */
   private IndexDescriptor createIndexDescriptor(final byte[] tuple) {
      final Schema indexesSchema = SystemCatalogSchema.INDEXES_SCHEMA;
      final Schema indexKeysSchema = SystemCatalogSchema.INDEXKEYS_SCHEMA;
      final int indexID = indexesSchema.getIntField(tuple, Column.INDEXES_IDXID.getPosition());
      final String name = indexesSchema.getCharField(tuple, Column.INDEXES_NAME.getPosition());
      final boolean clustered = indexesSchema.getIntField(tuple, Column.INDEXES_CLUSTERED.getPosition()) == 0
            ? false : true;
      final IndexType type = IndexType.values()[indexesSchema.getIntField(tuple,
            Column.INDEXES_TYPE.getPosition())];
      final int tableID = indexesSchema.getIntField(tuple, Column.INDEXES_TABID.getPosition());
      final int cardinality = 0;
      final int uniqueCardinality = 0;
      final IndexStatistics statistics = new IndexStatistics(cardinality, uniqueCardinality);
      // Read index keys
      final List<Record> indexKeys = this.getIndexKeyRecords(indexID, tableID);
      final int[] keyColumnIDs = new int[indexKeys.size()];
      final SortOrder[] keyOrders = new SortOrder[indexKeys.size()];
      for (final Record indexKey : indexKeys) {
         final byte[] keyTuple = indexKey.getValue();
         final int keyNo = indexKeysSchema.getIntField(keyTuple, Column.INDEXKEYS_KEYNO.getPosition());
         keyColumnIDs[keyNo] = indexKeysSchema.getIntField(keyTuple, Column.INDEXKEYS_COLID.getPosition());
         keyOrders[keyNo] = SortOrder.values()[indexKeysSchema.getIntField(keyTuple,
               Column.INDEXKEYS_ORDER.getPosition())];
      }
      final IndexDescriptor descriptor = new IndexDescriptor(this, statistics, indexID, name, type, clustered,
            keyColumnIDs, keyOrders, tableID);
      this.putDescriptor(descriptor);
      return descriptor;
   }

   @Override
   public int createBitmapIndex(final IndexStatistics statistics, final String name, final int[] keyColumnIDs,
         final int tableID) {
      throw new UnsupportedOperationException("Minibase does currently not support bitmap indexes.");
   }

   @Override
   public int createBitmapJoinIndex(final IndexStatistics statistics, final String name,
         final int[] keyColumnIDs, final int[][] referencesIDs, final Expression predicate,
         final int tableID) {
      throw new UnsupportedOperationException("Minibase does currently not support bitmap join indexes.");
   }

   @Override
   public BitmapIndexDescriptor getBitmapIndexDescriptor(final int indexID) {
      throw new UnsupportedOperationException("Minibase does currently not support bitmap indexes.");
   }

   @Override
   public boolean hasBitmapIndex(final String name) {
      throw new UnsupportedOperationException("Minibase does currently not support bitmap indexes.");
   }

   @Override
   public BitmapIndexDescriptor getBitmapIndex(final String name) {
      throw new UnsupportedOperationException("Minibase does currently not support bitmap indexes.");
   }

   @Override
   public Collection<BitmapIndexDescriptor> getBitmapIndexes(final int tableID) {
      throw new UnsupportedOperationException("Minibase does currently not support bitmap indexes.");
   }

   @Override
   public void createIndexKey(final int indexID, final int tableID, final int[] keyColumnIDs,
         final SortOrder[] keyOrders) {
      if (keyColumnIDs.length != keyOrders.length) {
         throw new IllegalStateException("Number of key columns and sort orders does not match.");
      }
      if (indexID == INVALID_ID && !this.getIndexKeyRecords(indexID, tableID).isEmpty()) {
         throw new IllegalStateException("Index key for table " + tableID + " exists already.");
      }
      if (indexID != INVALID_ID && !this.getIndexKeyRecords(indexID).isEmpty()) {
         throw new IllegalStateException("Index key for owner " + indexID + " exists already.");
      }
      final byte[] tuple = SystemCatalogSchema.INDEXKEYS_SCHEMA.newTuple();
      for (int i = 0; i < keyColumnIDs.length; i++) {
         SystemCatalogSchema.INDEXKEYS_SCHEMA.setAllFields(tuple, indexID == tableID ? INVALID_ID : indexID,
               tableID, keyColumnIDs[i], keyOrders[i].ordinal(), i);
         this.tables.get(Table.INDEXKEYS).insertRecord(tuple);
      }
   }

   /**
    * Drops the index key represented by the given record from the system catalog.
    *
    * @param indexKey
    *           index key record
    */

   private void dropIndexKey(final Record indexKey) {
      final byte[] tuple = indexKey.getValue();
      int ownerID = SystemCatalogSchema.INDEXKEYS_SCHEMA.getIntField(tuple,
            Column.INDEXKEYS_IDXID.getPosition());
      if (ownerID == INVALID_ID) {
         ownerID = SystemCatalogSchema.INDEXKEYS_SCHEMA.getIntField(tuple,
               Column.INDEXKEYS_TABID.getPosition());
      }
      final Optional<IndexKeyDescriptor> descriptor = this.getDescriptor(IndexKeyDescriptor.class, ownerID);
      if (descriptor.isPresent()) {
         this.removeDescriptor(descriptor.get());
      }
      this.tables.get(Table.INDEXKEYS).deleteRecord(indexKey.getRecordID());
   }

   @Override
   public IndexKeyDescriptor getIndexKey(final int ownerID) {
      final List<IndexKeyDescriptor> keys = this.getDescriptors(IndexKeyDescriptor.class, ownerID);
      if (!keys.isEmpty()) {
         return this.assertContainsOneOrNone(keys);
      }
      List<Record> records = this.getIndexKeyRecords(ownerID);
      if (records.isEmpty()) {
         records = this.getIndexKeyRecords(INVALID_ID, ownerID);
      }
      final int[] keyColumnIDs = new int[records.size()];
      final SortOrder[] keyOrders = new SortOrder[records.size()];
      final Schema schema = SystemCatalogSchema.INDEXKEYS_SCHEMA;
      for (final Record record : records) {
         final byte[] tuple = record.getValue();
         final int keyNo = schema.getIntField(tuple, Column.INDEXKEYS_KEYNO.getPosition());
         keyColumnIDs[keyNo] = schema.getIntField(tuple, Column.INDEXKEYS_COLID.getPosition());
         keyOrders[keyNo] = SortOrder.values()[schema.getIntField(tuple,
               Column.INDEXKEYS_ORDER.getPosition())];
      }
      final IndexKeyDescriptor descriptor = new IndexKeyDescriptor(this, ownerID, ownerID, keyColumnIDs,
            keyOrders);
      this.putDescriptor(descriptor);
      return descriptor;
   }

   /**
    * Tries to read the auto-increment counters.
    *
    * @return initialized counter map
    */
   private Counters initCounters() {
      final Counters counters = new Counters();
      final Schema tables = SystemCatalogSchema.TABLES_SCHEMA;
      try (FileScan scan = this.tables.get(Table.TABLES).openScan()) {
         while (scan.hasNext()) {
            final byte[] tuple = scan.next();
            final String name = tables.getCharField(tuple, Column.TABLES_NAME.getPosition());
            final int idctr = tables.getIntField(tuple, Column.TABLES_IDCTR.getPosition());
            if (idctr != INVALID_ID) {
               counters.initCounter(name, idctr);
            }
         }
      }
      return counters;
   }

   /**
    * Updates the auto-increment counter for the given table using the given record.
    *
    * @param table
    *           table to update
    * @param record
    *           corresponding table record
    */
   private void updateCounter(final Table table, final Record record) {
      SystemCatalogSchema.TABLES_SCHEMA.setField(record.getValue(), Column.TABLES_IDCTR.getPosition(),
            this.counters.getValue(table.getName()));
      this.tables.get(Table.TABLES).updateRecord(record.getRecordID(), record.getValue());
   }

   /**
    * Initializes the system catalog.
    */
   private void bootstrap() {
      // populate the tables table
      final int tablesID = this.createTable(Table.TABLES);
      final int columnsID = this.createTable(Table.COLUMNS);
      final int constraintsID = this.createTable(Table.CONSTRAINTS);
      final int primarykeysID = this.createTable(Table.PRIMARYKEYS);
      final int foreignkeysID = this.createTable(Table.FOREIGNKEYS);
      final int indexesID = this.createTable(Table.INDEXES);
      final int indexkeysID = this.createTable(Table.INDEXKEYS);
      Record record = this.getTableRecord(Table.TABLES);
      SystemCatalogSchema.TABLES_SCHEMA.setIntField(record.getValue(), Column.TABLES_IDCTR.getPosition(),
            indexkeysID);
      this.tables.get(Table.TABLES).updateRecord(record.getRecordID(), record.getValue());
      // populate the columns table
      final int pkTablesTabID = this.createColumn(Column.TABLES_TABID, tablesID);
      this.createColumn(Column.TABLES_NAME, tablesID);
      this.createColumn(Column.TABLES_ORDER, tablesID);
      this.createColumn(Column.TABLES_CARD, tablesID);
      this.createColumn(Column.TABLES_IDCTR, tablesID);
      final int pkColumnsColID = this.createColumn(Column.COLUMNS_COLID, columnsID);
      final int fkColumnsTabID = this.createColumn(Column.COLUMNS_TABID, columnsID);
      this.createColumn(Column.COLUMNS_COLNO, columnsID);
      this.createColumn(Column.COLUMNS_NAME, columnsID);
      this.createColumn(Column.COLUMNS_TYPE, columnsID);
      this.createColumn(Column.COLUMNS_SIZE, columnsID);
      this.createColumn(Column.COLUMNS_CARD, columnsID);
      this.createColumn(Column.COLUMNS_UCARD, columnsID);
      this.createColumn(Column.COLUMNS_MIN, columnsID);
      this.createColumn(Column.COLUMNS_MAX, columnsID);
      final int pkConstraintsConstID = this.createColumn(Column.CONSTRAINTS_CONSTID, constraintsID);
      final int fkConstraintsTabID = this.createColumn(Column.CONSTRAINTS_TABID, constraintsID);
      final int fkConstraintsColID = this.createColumn(Column.CONSTRAINTS_COLID, constraintsID);
      this.createColumn(Column.CONSTRAINTS_NAME, constraintsID);
      this.createColumn(Column.CONSTRAINTS_TYPE, constraintsID);
      final int fkPrimarykeysConstID = this.createColumn(Column.PRIMARYKEYS_CONSTID, primarykeysID);
      final int fkPrimarykeysTabID = this.createColumn(Column.PRIMARYKEYS_TABID, primarykeysID);
      final int fkPrimarykeysColID = this.createColumn(Column.PRIMARYKEYS_COLID, primarykeysID);
      this.createColumn(Column.PRIMARYKEYS_KEYNO, primarykeysID);
      final int fkForeignkeysConstID = this.createColumn(Column.FOREIGNKEYS_CONSTID, foreignkeysID);
      final int fkForeignkeysFKeyID = this.createColumn(Column.FOREIGNKEYS_FKEYID, foreignkeysID);
      final int fkForeignkeysRKeyID = this.createColumn(Column.FOREIGNKEYS_RKEYID, foreignkeysID);
      final int fkForeignkeysFKey = this.createColumn(Column.FOREIGNKEYS_FKEY, foreignkeysID);
      final int fkForeignkeysRKey = this.createColumn(Column.FOREIGNKEYS_RKEY, foreignkeysID);
      this.createColumn(Column.FOREIGNKEYS_KEYNO, foreignkeysID);
      final int pkIndexesIdxID = this.createColumn(Column.INDEXES_IDXID, indexesID);
      final int fkIndexesTabID = this.createColumn(Column.INDEXES_TABID, indexesID);
      this.createColumn(Column.INDEXES_NAME, indexesID);
      this.createColumn(Column.INDEXES_TYPE, indexesID);
      this.createColumn(Column.INDEXES_CLUSTERED, indexesID);
      final int fkIndexkeysIdxID = this.createColumn(Column.INDEXKEYS_IDXID, indexkeysID);
      final int fkIndexkeysTabID = this.createColumn(Column.INDEXKEYS_TABID, indexkeysID);
      final int fkIndexkeysColID = this.createColumn(Column.INDEXKEYS_COLID, indexkeysID);
      this.createColumn(Column.INDEXKEYS_ORDER, indexkeysID);
      int lastID = this.createColumn(Column.INDEXKEYS_KEYNO, indexkeysID);
      record = this.getTableRecord(Table.COLUMNS);
      SystemCatalogSchema.TABLES_SCHEMA.setIntField(record.getValue(), Column.TABLES_IDCTR.getPosition(),
            lastID);
      this.tables.get(Table.TABLES).updateRecord(record.getRecordID(), record.getValue());
      // create primary keys
      this.createColumnPrimaryKey("pk_systables", tablesID, pkTablesTabID);
      this.createColumnPrimaryKey("pk_syscolumns", columnsID, pkColumnsColID);
      this.createColumnPrimaryKey("pk_sysconstraints", constraintsID, pkConstraintsConstID);
      lastID = this.createColumnPrimaryKey("pk_indexes", indexesID, pkIndexesIdxID);
      record = this.getTableRecord(Table.CONSTRAINTS);
      SystemCatalogSchema.TABLES_SCHEMA.setIntField(record.getValue(), Column.TABLES_IDCTR.getPosition(),
            lastID);
      this.tables.get(Table.TABLES).updateRecord(record.getRecordID(), record.getValue());
      // create foreign keys
      this.createColumnForeignKey("fk_syscolumns_tabid", columnsID, fkColumnsTabID, tablesID, pkTablesTabID);
      this.createColumnForeignKey("fk_sysconstraints_tabid", constraintsID, fkConstraintsTabID, tablesID,
            pkTablesTabID);
      this.createColumnForeignKey("fk_sysconstraints_colid", constraintsID, fkConstraintsColID, columnsID,
            pkColumnsColID);
      this.createColumnForeignKey("fk_sysprimarykeys_constid", primarykeysID, fkPrimarykeysConstID,
            constraintsID, pkConstraintsConstID);
      this.createColumnForeignKey("fk_sysprimarykeys_tabid", primarykeysID, fkPrimarykeysTabID, tablesID,
            pkTablesTabID);
      this.createColumnForeignKey("fk_sysprimarykeys_colid", primarykeysID, fkPrimarykeysColID, columnsID,
            pkColumnsColID);
      this.createColumnForeignKey("fk_sysforeignkeys_constid", foreignkeysID, fkForeignkeysConstID,
            constraintsID, pkConstraintsConstID);
      this.createColumnForeignKey("fk_sysforeignkeys_fkeyid", foreignkeysID, fkForeignkeysFKeyID, tablesID,
            pkTablesTabID);
      this.createColumnForeignKey("fk_sysforeignkeys_rkeyid", foreignkeysID, fkForeignkeysRKeyID, tablesID,
            pkTablesTabID);
      this.createColumnForeignKey("fk_sysforeignkeys_fkey", foreignkeysID, fkForeignkeysFKey, columnsID,
            pkColumnsColID);
      this.createColumnForeignKey("fk_sysforeignkeys_rkey", foreignkeysID, fkForeignkeysRKey, columnsID,
            pkColumnsColID);
      this.createColumnForeignKey("fk_sysindexes_tabid", indexesID, fkIndexesTabID, tablesID, pkTablesTabID);
      this.createColumnForeignKey("fk_sysindexkeys_idxid", indexkeysID, fkIndexkeysIdxID, indexkeysID,
            pkIndexesIdxID);
      this.createColumnForeignKey("fk_sysindexkeys_tabid", indexkeysID, fkIndexkeysTabID, tablesID,
            pkTablesTabID);
      lastID = this.createColumnForeignKey("fk_sysindexkeys_colid", indexkeysID, fkIndexkeysColID, columnsID,
            pkColumnsColID);
      record = this.getTableRecord(Table.CONSTRAINTS);
      SystemCatalogSchema.TABLES_SCHEMA.setIntField(record.getValue(), Column.TABLES_IDCTR.getPosition(),
            lastID);
      this.tables.get(Table.TABLES).updateRecord(record.getRecordID(), record.getValue());
      // update table and column statistics
      this.initStats(Table.TABLES, SystemCatalogSchema.TABLES_SCHEMA, Column.TABLES_TABID);
      this.initStats(Table.COLUMNS, SystemCatalogSchema.COLUMNS_SCHEMA, Column.COLUMNS_COLID);
      this.initStats(Table.CONSTRAINTS, SystemCatalogSchema.CONSTRAINTS_SCHEMA, Column.CONSTRAINTS_CONSTID);
      this.initStats(Table.PRIMARYKEYS, SystemCatalogSchema.PRIMARYKEYS_SCHEMA, Column.PRIMARYKEYS_CONSTID);
      this.initStats(Table.FOREIGNKEYS, SystemCatalogSchema.FOREIGNKEYS_SCHEMA, Column.FOREIGNKEYS_CONSTID);
      this.initStats(Table.INDEXES, SystemCatalogSchema.INDEXES_SCHEMA, Column.INDEXES_IDXID);
      this.initStats(Table.INDEXKEYS, SystemCatalogSchema.INDEXKEYS_SCHEMA, Column.INDEXKEYS_IDXID);
   }

   /**
    * Creates a table from the given schema table with empty table statistics.
    *
    * @param table
    *           system catalog schema entry of the table
    * @return ID of the table
    */
   private int createTable(final Table table) {
      return this.createTable(TableStatistics.emptyStatistics(), table.getName());
   }

   /**
    * Creates a column from the given schema column in the given table with empty column statistics.
    *
    * @param column
    *           system catalog schema entry of the column
    * @param tableID
    *           table ID of the table containing the column
    * @return ID of the column
    */
   private int createColumn(final Column column, final int tableID) {
      return this.createColumn(ColumnStatistics.emptyStatistics(), column.getName(), column.getType(),
            column.getSize(), tableID);
   }

   /**
    * Initializes the table and column statistics of the given table.
    *
    * @param table
    *           table for which statistics will be initialized
    * @param schema
    *           schema of the table
    * @param identifier
    *           system catalog schema entry of the identifier column
    */
   private void initStats(final Table table, final Schema schema, final Column identifier) {
      int card = 0;
      final int[] ucards = new int[schema.getColumnCount()];
      @SuppressWarnings("unchecked")
      final Set<Object>[] uniques = new Set[schema.getColumnCount()];
      for (int i = 0; i < uniques.length; i++) {
         uniques[i] = new HashSet<>();
      }
      final float[] minimums = new float[schema.getColumnCount()];
      final float[] maximums = new float[schema.getColumnCount()];
      Arrays.fill(minimums, Float.POSITIVE_INFINITY);
      Arrays.fill(maximums, Float.NEGATIVE_INFINITY);
      try (FileScan scan = this.tables.get(table).openScan()) {
         while (scan.hasNext()) {
            final byte[] data = scan.next();
            final Object[] values = schema.getAllFields(data);
            int field = 0;
            for (final Object value : values) {
               if (!uniques[field].contains(value)) {
                  uniques[field].add(value);
                  ucards[field]++;
               }
               if (value instanceof Number) {
                  final float floatValue = ((Number) value).floatValue();
                  minimums[field] = floatValue < minimums[field] ? floatValue : minimums[field];
                  maximums[field] = floatValue > maximums[field] ? floatValue : maximums[field];
               }
               field++;
            }
            card++;
         }
      }

      final Record record = this.getTableRecord(table);
      final int tableID = SystemCatalogSchema.TABLES_SCHEMA.getIntField(record.getValue(),
            identifier.getPosition());
      SystemCatalogSchema.TABLES_SCHEMA.setIntField(record.getValue(), Column.TABLES_CARD.getPosition(),
            card);
      this.tables.get(Table.TABLES).updateRecord(record.getRecordID(), record.getValue());

      final Schema columnsSchema = SystemCatalogSchema.COLUMNS_SCHEMA;
      for (int field = 0; field < schema.getColumnCount(); field++) {
         final Record column = this.getColumnRecord(schema.getColumn(field).getName(), tableID).get();
         final byte[] columnTuple = column.getValue();
         columnsSchema.setIntField(columnTuple, Column.COLUMNS_CARD.getPosition(), card);
         columnsSchema.setIntField(columnTuple, Column.COLUMNS_UCARD.getPosition(), ucards[field]);
         if (minimums[field] < Float.POSITIVE_INFINITY) {
            columnsSchema.setFloatField(columnTuple, Column.COLUMNS_MIN.getPosition(), minimums[field]);
         }
         if (maximums[field] > Float.NEGATIVE_INFINITY) {
            columnsSchema.setFloatField(columnTuple, Column.COLUMNS_MAX.getPosition(), maximums[field]);
         }
         this.tables.get(Table.COLUMNS).updateRecord(column.getRecordID(), columnTuple);
      }
   }

   /**
    * Returns the record that corresponds to the entry of the given table in the tables table from the cache.
    * If the record is not already cached, it will be read from disk and cached.
    *
    * @param table
    *           system catalog schema entry of the table
    * @return table record
    */
   private Record getTableRecord(final Table table) {
      Record record = this.tablesRecords.get(table);
      if (record == null) {
         final Optional<Record> optional = this.getTableRecord(table.getName());
         if (optional.isPresent()) {
            record = optional.get();
            this.tablesRecords.put(table, record);
         } else {
            throw new IllegalStateException("Table " + table.getName() + " does not exist.");
         }
      }
      return record;
   }

   /**
    * Returns the record in the tables table that corresponds to the table with the given identifier.
    *
    * @param tableID
    *           identifier of the table
    * @return table record
    */
   private Optional<Record> getTableRecord(final int tableID) {
      return this.getRecord(Table.TABLES, new Column[] { Column.TABLES_TABID }, new Object[] { tableID });
   }

   /**
    * Returns the record in the tables table that corresponds to the table with the given name.
    *
    * @param name
    *           name of the table
    * @return table record
    */
   private Optional<Record> getTableRecord(final String name) {
      return this.getRecord(Table.TABLES, new Column[] { Column.TABLES_NAME }, new Object[] { name });
   }

   /**
    * Returns the record in the columns table that corresponds to the column with the given identifier.
    *
    * @param columnID
    *           identifier of the column
    * @return column record
    */
   private Optional<Record> getColumnRecord(final int columnID) {
      return this.getRecord(Table.COLUMNS, new Column[] { Column.COLUMNS_COLID }, new Object[] { columnID });
   }

   /**
    * Returns the record in the columns table that corresponds to the column with the given name in the given
    * table.
    *
    * @param name
    *           name of the column
    * @param tableID
    *           table of the column
    * @return column record
    */
   private Optional<Record> getColumnRecord(final String name, final int tableID) {
      return this.getRecord(Table.COLUMNS, new Column[] { Column.COLUMNS_NAME, Column.COLUMNS_TABID },
            new Object[] { name, tableID });
   }

   /**
    * Returns a list of records from the column table that correspond to the columns of the table with the
    * given identifier.
    *
    * @param tableID
    *           identifier of the table
    * @return list of column records
    */
   private List<Record> getColumnRecords(final int tableID) {
      return this.getRecords(Table.COLUMNS, new Column[] { Column.COLUMNS_TABID }, new Object[] { tableID });
   }

   /**
    * Returns the record in the constraints table that corresponds to the constraint with the given
    * identifier.
    *
    * @param constraintID
    *           identifier of the constraint
    * @return constraint record
    */
   private Optional<Record> getConstraintRecord(final int constraintID) {
      return this.getRecord(Table.CONSTRAINTS, new Column[] { Column.CONSTRAINTS_CONSTID },
            new Object[] { constraintID });
   }

   /**
    * Returns the record in the constraints table that corresponds to the constraint with the given identifier
    * and type.
    *
    * @param constraintID
    *           identifier of the constraint
    * @param type
    *           type of the constraint
    * @return constraint record
    */

   private Optional<Record> getConstraintRecord(final int constraintID, final ConstraintType type) {
      return this.getRecord(Table.CONSTRAINTS,
            new Column[] { Column.CONSTRAINTS_CONSTID, Column.CONSTRAINTS_TYPE },
            new Object[] { constraintID, type.ordinal() });
   }

   /**
    * Returns the record in the constraints table that corresponds to the constraint with the given name.
    *
    * @param name
    *           name of the constraint
    * @return constraint record
    */
   private Optional<Record> getConstraintRecord(final String name) {
      return this.getRecord(Table.CONSTRAINTS, new Column[] { Column.CONSTRAINTS_NAME },
            new Object[] { name });
   }

   /**
    * Returns the record in the constraints table that corresponds to the constraint with the given name and
    * type.
    *
    * @param name
    *           name of the constraint
    * @param type
    *           type of the constraint
    * @return constraint record
    */
   private Optional<Record> getConstraintRecord(final String name, final ConstraintType type) {
      return this.getRecord(Table.CONSTRAINTS,
            new Column[] { Column.CONSTRAINTS_NAME, Column.CONSTRAINTS_TYPE },
            new Object[] { name, type.ordinal() });
   }

   /**
    * Returns the records in the constraints table that are owned by the table with the given identifier and
    * have the given type.
    *
    * @param tableID
    *           identifier of owner table of the constraint
    * @param type
    *           type of the constraint
    * @return list of constraint records
    */
   private List<Record> getConstraintRecords(final int tableID, final ConstraintType type) {
      return this.getRecords(Table.CONSTRAINTS,
            new Column[] { Column.CONSTRAINTS_TABID, Column.CONSTRAINTS_TYPE },
            new Object[] { tableID, type.ordinal() });
   }

   /**
    * Returns the records in the constraints table that are owned by the table and column with the given
    * identifiers and that have the given type.
    *
    * @param tableID
    *           identifier of the owner table of the constraint
    * @param columnID
    *           identifier of the owner column of the constraint
    * @param type
    *           type of the constraint
    * @return list of constraint records
    */
   private List<Record> getConstraintRecords(final int tableID, final int columnID,
         final ConstraintType type) {
      return this.getRecords(Table.CONSTRAINTS,
            new Column[] { Column.CONSTRAINTS_TABID, Column.CONSTRAINTS_COLID, Column.CONSTRAINTS_TYPE },
            new Object[] { tableID, columnID, type.ordinal() });
   }

   /**
    * Returns the record in the indexes table that corresponds to the index with the given identifier.
    *
    * @param indexID
    *           identifier of the index
    * @return index record
    */
   private Optional<Record> getIndexRecord(final int indexID) {
      return this.getRecord(Table.INDEXES, new Column[] { Column.INDEXES_IDXID }, new Object[] { indexID });
   }

   /**
    * Returns the record in the indexes table that corresponds to the index with the given name.
    *
    * @param name
    *           name of the index
    * @return index record
    */
   private Optional<Record> getIndexRecord(final String name) {
      return this.getRecord(Table.INDEXES, new Column[] { Column.INDEXES_NAME }, new Object[] { name });
   }

   /**
    * Returns a list of records from the index table that correspond to the indexes of the table with the
    * given identifier.
    *
    * @param tableID
    *           identifier of the table
    * @return list of index records
    */
   private List<Record> getIndexRecords(final int tableID) {
      return this.getRecords(Table.INDEXES, new Column[] { Column.INDEXES_TABID }, new Object[] { tableID });
   }

   /**
    * Returns the records in the indexes table that corresponds to the index key with the given owner
    * identifier.
    *
    * @param ownerID
    *           owner identifier
    * @return list of index key records
    */
   private List<Record> getIndexKeyRecords(final int ownerID) {
      return this.getRecords(Table.INDEXKEYS, new Column[] { Column.INDEXKEYS_IDXID },
            new Object[] { ownerID });
   }

   /**
    * Returns the records in the indexes table that corresponds to the index key for the given index and table
    * identifier.
    *
    * @param indexID
    *           identifier of the index
    * @param tableID
    *           identifier of the table
    * @return list of index key records
    */
   private List<Record> getIndexKeyRecords(final int indexID, final int tableID) {
      return this.getRecords(Table.INDEXKEYS, new Column[] { Column.INDEXKEYS_IDXID, Column.INDEXKEYS_TABID },
            new Object[] { indexID, tableID });
   }

   /**
    * Performs a sequential scan of the given heap file to look for a record that matches the given values on
    * the given columns.
    *
    * @param table
    *           system catalog schema table entry
    * @param columns
    *           system catalog schema column entry
    * @param values
    *           field values
    * @return record if found, {@link Optional#empty()} otherwise
    */
   private Optional<Record> getRecord(final Table table, final Column[] columns, final Object[] values) {
      final List<Record> records = this.getRecords(table, columns, values);
      if (records.size() > 0) {
         return Optional.of(records.get(0));
      }
      return Optional.empty();
   }

   /**
    * Performs a sequential scan of the table to look for records that matches the given values on the field
    * with the given positions.
    *
    * @param table
    *           system catalog schema table entry
    * @param columns
    *           system catalog schema column entry
    * @param values
    *           field values
    * @return record if found, {@code null} otherwise
    */
   private List<Record> getRecords(final Table table, final Column[] columns, final Object[] values) {
      if (values == null || columns == null || values.length != columns.length) {
         throw new IllegalStateException("Positions and values have to match and cannot be null.");
      }
      final File file = this.tables.get(table);
      final Schema schema = this.schemas.get(table);
      final List<Record> result = new ArrayList<>();
      try (FileScan scan = file.openScan()) {
         while (scan.hasNext()) {
            final byte[] tuple = scan.next();
            final Record record = new Record(scan.lastID(), tuple);
            final Object[] objects = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
               objects[i] = schema.getField(tuple, columns[i].getPosition());
            }
            if (Arrays.equals(values, objects)) {
               result.add(record);
            }
         }
      }
      return result;
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder("----- Minibase System Catalog -----\n\n");
      result.append(this.toString(Table.TABLES, SystemCatalogSchema.TABLES_SCHEMA));
      result.append(this.toString(Table.COLUMNS, SystemCatalogSchema.COLUMNS_SCHEMA));
      result.append(this.toString(Table.CONSTRAINTS, SystemCatalogSchema.CONSTRAINTS_SCHEMA));
      result.append(this.toString(Table.PRIMARYKEYS, SystemCatalogSchema.PRIMARYKEYS_SCHEMA));
      result.append(this.toString(Table.FOREIGNKEYS, SystemCatalogSchema.FOREIGNKEYS_SCHEMA));
      result.append(this.toString(Table.INDEXES, SystemCatalogSchema.INDEXES_SCHEMA));
      result.append(this.toString(Table.INDEXKEYS, SystemCatalogSchema.INDEXKEYS_SCHEMA));
      return result.toString();
   }

   /**
    * Returns a string representation of the given table according to the given schema.
    *
    * @param table
    *           the table
    * @param schema
    *           schema of the table
    * @return string representation of the table
    */
   private String toString(final Table table, final Schema schema) {
      final File file = this.tables.get(table);
      final StringBuilder result = new StringBuilder();
      result.append(file.getName().orElse("temporary file") + "\n");
      result.append(schema.toString());
      try (FileScan scan = file.openScan()) {
         while (scan.hasNext()) {
            final byte[] data = scan.next();
            result.append(schema.showTuple(data) + "\n");
         }
         result.append("\n");
      }
      return result.toString();
   }

   /**
    * For each table an identifier counter is maintained that is used to compute the next identifier for that
    * table.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private class Counters {

      /** Map of identifier counters. */
      private final Map<String, Counter> counters;

      /**
       * Creates a new map of identifier counters.
       */
      Counters() {
         this.counters = new HashMap<>();
      }

      /**
       * Creates a new counter with the given initial value for the table with the given identifier.
       *
       * @param key
       *           name of the table
       * @param value
       *           initial counter value
       */
      void initCounter(final String key, final int value) {
         if (this.counters.containsKey(key)) {
            this.counters.remove(key);
         }
         this.counters.put(key, new Counter(value));
      }

      /**
       * Returns the current counter value for the table with the given identifier. If no counter exists for
       * this table, a new counter is created.
       *
       * @param key
       *           name of the table
       * @return current counter value
       */
      int getValue(final String key) {
         return this.getCounter(key).getValue();
      }

      /**
       * Returns the next counter value for the table with the given identifier. If no counter exists for this
       * table, a new counter is created.
       *
       * @param key
       *           name of the table
       * @return next counter value
       */
      int getNextID(final String key) {
         return this.getCounter(key).getNextID();
      }

      /**
       * Returns the number of counters that have been defined.
       *
       * @return number of counters
       */
      int size() {
         return this.counters.size();
      }

      /**
       * Returns the counter for the table with the given identifier. If no counter exists for this table, a
       * new one is created.
       *
       * @param key
       *           name of the table
       * @return counter for the corresponding table
       */
      private Counter getCounter(final String key) {
         Counter counter = this.counters.get(key);
         if (counter == null) {
            counter = new Counter(0);
            this.counters.put(key, counter);
         }
         return counter;
      }
   }

   /**
    * A simple integer counter that is used to compute identifiers.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private class Counter {

      /** Counter value. */
      private int value;

      /**
       * Creates a new counter with the given initial value.
       *
       * @param value
       *           initial value
       */
      Counter(final int value) {
         this.value = value;
      }

      /**
       * Returns the current value of the counter.
       *
       * @return current counter value
       */
      int getValue() {
         return this.value;
      }

      /**
       * Increments and returns the counter value.
       *
       * @return next counter value
       */
      int getNextID() {
         return this.value++;
      }
   }

   /**
    * Comparator that uses a given column to compare two tuples.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   @SuppressWarnings("unused")
   private class TupleComparator implements Comparator<byte[]> {

      /** Schema of the tuples. */
      private final Schema schema;

      /** Position of the column to compare. */
      private final Column column;

      /**
       * Creates a new tuple comparator that uses the given column to compare tuples.
       *
       * @param schema
       *           schema of the tuples
       * @param column
       *           column to compare on
       */
      TupleComparator(final Schema schema, final Column column) {
         this.schema = schema;
         this.column = column;
      }

      @Override
      @SuppressWarnings("unchecked")
      public int compare(final byte[] aTuple, final byte[] bTuple) {
         final Object aValue = this.schema.getField(aTuple, this.column.getPosition());
         final Object bValue = this.schema.getField(bTuple, this.column.getPosition());
         return ((Comparable<Object>) aValue).compareTo(bValue);
      }
   }

   /**
    * A record combines a record ID with its current value.
    */
   private static final class Record {

      /** The record's ID. */
      private final RecordID recordID;
      /** The record's tuple. */
      private final byte[] value;

      /**
       * Constructs a record with the given ID and value.
       *
       * @param recordID
       *           record ID
       * @param value
       *           record value
       */
      Record(final RecordID recordID, final byte[] value) {
         this.recordID = recordID;
         this.value = value;
      }

      /**
       * Returns this record's ID.
       *
       * @return record ID
       */
      public RecordID getRecordID() {
         return this.recordID;
      }

      /**
       * Returns this record's value.
       *
       * @return current value
       */
      public byte[] getValue() {
         return this.value;
      }
   }
}

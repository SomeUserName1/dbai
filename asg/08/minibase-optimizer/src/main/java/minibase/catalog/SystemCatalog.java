/*
 * @(#)SystemCatalog.java   1.0   Aug 23, 2014
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
import java.util.List;

import minibase.query.optimizer.Expression;

/**
 * The Minibase system catalog contains metadata about tables, columns, constraints, and indexes. Whereas this
 * metadata is created and managed exclusively through the system catalog, it can be retrieved through a
 * number of read-only catalog descriptors. The methods of this interface obey the following naming
 * convention. A first group of methods is dedicated to schema definition and evolution. In analogy to SQL,
 * this methods start with {@code create*(...)}, {@code alter*(int, ...)}, and {@code drop*(int)}. A second
 * group of methods supports the retrieval of system catalog descriptors by identifier. These methods are
 * named {@code get*Descriptor(int)}. Finally, a last group of methods implements frequent "queries" for
 * system catalog descriptors to retrieve them by name or some other criterion. These methods are simply named
 * {@code get*(...)}.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public interface SystemCatalog {

   /** Constant to denote an invalid ID. */
   int INVALID_ID = -1;

   /**
    * Returns the page size used by this system catalog in bytes.
    * 
    * @return page size in bytes
    */
   int getPageSize();

   /**
    * Creates a new table with the given name and returns its identifier in the system catalog.
    *
    * @param statistics
    *           statistics of the table
    * @param name
    *           name of the table
    * @param order
    *           data order of the table
    * @return identifier of the table
    */
   int createTable(TableStatistics statistics, String name, DataOrder order);

   /**
    * Creates a new table with the given name and returns its identifier in the system catalog.
    *
    * @param statistics
    *           statistics of the table
    * @param name
    *           name of the table
    * @return identifier of the table
    */
   int createTable(TableStatistics statistics, String name);

   /**
    * Drops the table with the given identifier from the system catalog.
    *
    * @param tableID
    *           identifier of the table
    */
   void dropTable(int tableID);

   /**
    * Drops the table with the given name from the system catalog.
    *
    * @param name
    *           name of the table
    */
   void dropTable(String name);

   /**
    * Returns the table descriptor for the table with the given identifier.
    *
    * @param tableID
    *           identifier of the table descriptor
    * @return table descriptor
    */
   TableDescriptor getTableDescriptor(int tableID);

   /**
    * Checks if a table with the given name exists.
    *
    * @param name
    *           table name
    * @return {@code true} if the table exists, {@code false} otherwise
    */
   boolean hasTable(String name);

   /**
    * Returns the table descriptor with the given name or {@code null} if no such table descriptor exists.
    *
    * @param name
    *           name of the table descriptor
    * @return corresponding descriptor or {@code null} if no such descriptor exists
    */
   TableDescriptor getTable(String name);

   /**
    * Returns an immutable list of table descriptors of all tables registered in the system catalog.
    *
    * @return an immutable set of all tables, never {@code null}
    */
   Collection<TableDescriptor> getTables();

   /**
    * Creates an index key for the object reference by the given catalog identifier using the given key
    * columns and sort orders. Index keys can be created for tables, indexes, and bitmap indexes.
    *
    * @param indexID
    *           identifier of the index that owns the index key, {@link SystemCatalog#INVALID_ID} if the index
    *           key is owned by a table
    * @param tableID
    *           identifier of the table containing the columns of the index key
    * @param keyColumnIDs
    *           identifiers of the columns of the index key
    * @param keyOrders
    *           sort order of the columns of the index key
    */
   void createIndexKey(int indexID, int tableID, int[] keyColumnIDs, SortOrder[] keyOrders);

   /**
    * Returns an optional of the index key descriptors owned by a catalog descriptor.
    *
    * @param ownerID
    *           identifier of the owner the index key
    * @return index key descriptor optional
    */
   IndexKeyDescriptor getIndexKey(int ownerID);

   /**
    * Creates a new index for the table with the given identifier that has the given name and search key.
    *
    * @param statistics
    *           statistics of the index
    * @param name
    *           name of the index
    * @param type
    *           type of the index
    * @param clustered
    *           indicates whether the index is clustered or not
    * @param keyColumnIDs
    *           identifiers of the search key columns of the index
    * @param keyOrders
    *           sort order of the search key columns of the index
    * @param tableID
    *           identifier of the table that is indexed by the index
    * @return identifier of the index
    */
   int createIndex(IndexStatistics statistics, String name, IndexType type, boolean clustered,
         int[] keyColumnIDs, SortOrder[] keyOrders, int tableID);

   /**
    * Creates a new index for the table with the given identifier that has the given name and search key.
    *
    * @param statistics
    *           statistics of the index
    * @param name
    *           name of the index
    * @param type
    *           type of the index
    * @param clustered
    *           indicates whether the index is clustered or not
    * @param keyColumnIDs
    *           identifiers of the search key columns of the index
    * @param tableID
    *           identifier of the table that is indexed by the index
    * @return identifier of the index
    */
   int createIndex(IndexStatistics statistics, String name, IndexType type, boolean clustered,
         int[] keyColumnIDs, int tableID);

   /**
    * Drops the index with the given identifier from the system catalog.
    *
    * @param indexID
    *           identifier of the index
    */
   void dropIndex(int indexID);

   /**
    * Drops the index with the given name from the system catalog.
    *
    * @param name
    *           name of the index
    */
   void dropIndex(String name);

   /**
    * Returns the index descriptor of the index with the given identifier.
    *
    * @param indexID
    *           identifier of the index
    * @return index descriptor
    */
   IndexDescriptor getIndexDescriptor(int indexID);

   /**
    * Checks if an index with the given name exists.
    *
    * @param name
    *           index name
    * @return {@code true} if the index exists, {@code false} otherwise
    */
   boolean hasIndex(String name);

   /**
    * Returns the index descriptor of the index with the given name in the table with the given identifier.
    *
    * @param name
    *           index name
    * @return index descriptor
    */
   IndexDescriptor getIndex(String name);

   /**
    * Returns all index descriptors of the table with the given ID as an immutable list.
    *
    * @param tableID
    *           identifier of the table
    * @return immutable list of index descriptors, never {@code null}
    */
   Collection<IndexDescriptor> getIndexes(int tableID);

   /**
    * Creates a new bitmap index for the table with the given identifier that has the given name and search
    * key.
    *
    * @param statistics
    *           statistics of the index
    * @param name
    *           name of the index
    * @param keyColumnIDs
    *           column IDs of the search key of the index
    * @param tableID
    *           ID of the table that is indexed by the index
    * @return globally unique ID of the index
    */
   int createBitmapIndex(IndexStatistics statistics, String name, int[] keyColumnIDs, int tableID);

   /**
    * Creates a new bitmap join index for the table (identified by the given ID) with the given name and
    * search key. The described bitmap join index is built based on the given set of column references and
    * supports the evaluation of the given predicate.
    *
    * @param statistics
    *           statistics of the index
    * @param name
    *           name of the index
    * @param keyColumnIDs
    *           column IDs of the search key of the index
    * @param referencesIDs
    *           array of IDs of groups of referenced columns used to build the index
    * @param predicate
    *           predicate supported by the index
    * @param tableID
    *           ID of the table that is indexed by the index
    * @return globally unique ID of the index
    */
   int createBitmapJoinIndex(IndexStatistics statistics, String name, int[] keyColumnIDs,
         int[][] referencesIDs, Expression predicate, int tableID);

   /**
    * Returns the bitmap index descriptor of the index with the given ID.
    *
    * @param indexID
    *           ID of the index
    * @return bitmap index descriptor
    */
   BitmapIndexDescriptor getBitmapIndexDescriptor(int indexID);

   /**
    * Checks if a bitmap index with the given name exists.
    *
    * @param name
    *           bitmap index name
    * @return {@code true} if the bitmap index exists, {@code false} otherwise
    */
   boolean hasBitmapIndex(String name);

   /**
    * Returns the bitmap index descriptor of the index with the given name in the table with the given ID.
    *
    * @param name
    *           bitmap index name
    * @return bitmap index descriptor
    */
   BitmapIndexDescriptor getBitmapIndex(String name);

   /**
    * Returns all bitmap index descriptors of the table with the given ID as an immutable list.
    *
    * @param tableID
    *           ID of the table
    * @return immutable list of bitmap index descriptors, never {@code null}
    */
   Collection<BitmapIndexDescriptor> getBitmapIndexes(int tableID);

   /**
    * Creates a new column of the table (identified by the given ID) with the given name, type and size of
    * values. The size parameter is ignored if the given column type already defines a size.
    *
    * @param statistics
    *           statistics of the column
    * @param name
    *           name of the column
    * @param type
    *           type of the column
    * @param size
    *           size of the column values, ignored if type already defines a size
    * @param tableID
    *           ID of the enclosing table
    * @return globally unique ID of the column
    */
   int createColumn(ColumnStatistics statistics, String name, DataType type, int size, int tableID);

   /**
    * Returns the column descriptor for the column with the given ID.
    *
    * @param columnID
    *           ID of the column descriptor
    * @return column descriptor
    */
   ColumnDescriptor getColumnDescriptor(int columnID);

   /**
    * Checks if a column with the given name exists in the given table.
    *
    * @param tableID
    *           table identifier
    * @param name
    *           column name
    * @return {@code true} if the column exists, {@code false} otherwise
    */
   boolean hasColumn(int tableID, String name);

   /**
    * Returns the columns descriptor for the column with the given name in the table with the given ID.
    *
    * @param tableID
    *           ID of the enclosing table
    * @param name
    *           name of the column
    * @return column descriptor
    */
   ColumnDescriptor getColumn(int tableID, String name);

   /**
    * Returns all column descriptors of the table with the given ID as an immutable list.
    *
    * @param tableID
    *           ID of the enclosing table
    * @return immutable list of column descriptors
    */
   List<ColumnDescriptor> getColumns(int tableID);

   /**
    * Creates a primary key as a column constraint with the given name and ID of the key column.
    *
    * @param name
    *           name of the constraint
    * @param tableID
    *           ID of the enclosing table
    * @param columnID
    *           ID of the key column
    * @return globally unique ID of the constraint
    */
   int createColumnPrimaryKey(String name, int tableID, int columnID);

   /**
    * Creates a primary key as a table constraint with the given name, ID of the enclosing table, and IDs of
    * the key columns.
    *
    * @param name
    *           name of the constraint
    * @param tableID
    *           ID of the enclosing table
    * @param columnIDs
    *           IDs of the key columns
    * @return globally unique ID of the constraint
    */
   int createTablePrimaryKey(String name, int tableID, int[] columnIDs);

   /**
    * Creates a foreign key as a column constraint with the given name, ID of the key column, and ID of the
    * referenced column.
    *
    * @param name
    *           name of the constraint
    * @param tableID
    *           ID of the enclosing table
    * @param columnID
    *           ID of the primary key column
    * @param referenceTableID
    *           ID of the table containing the referenced column
    * @param referenceID
    *           ID of the referenced column
    * @return globally unique ID of the constraint
    */
   int createColumnForeignKey(String name, int tableID, int columnID, int referenceTableID, int referenceID);

   /**
    * Creates a foreign key as a table constraint with the given name, ID of the enclosing table, IDs of the
    * key columns, and IDs of the referenced columns.
    *
    * @param name
    *           name of the constraint
    * @param tableID
    *           ID of the enclosing table
    * @param columnIDs
    *           IDs of the primary key columns
    * @param referenceTableID
    *           ID of the table containing the referenced columns
    * @param referenceIDs
    *           IDs of the referenced columns
    * @return globally unique ID of the constraint
    */
   int createTableForeignKey(String name, int tableID, int[] columnIDs, int referenceTableID,
         int[] referenceIDs);

   /**
    * Returns the constraint descriptor for the constraint with the given ID.
    *
    * @param constraintID
    *           ID of the constraint descriptor
    * @return constraint descriptor
    */
   ConstraintDescriptor getConstraintDescriptor(int constraintID);

   /**
    * Checks if a constraint with the given name exists.
    *
    * @param name
    *           constraint name
    * @return {@code true} if the constrain exists, {@code false} otherwise
    */
   boolean hasConstraint(String name);

   /**
    * Returns the constraint descriptor for the constraint with the given name.
    *
    * @param name
    *           name of the constraint
    * @return constraint descriptor
    */
   ConstraintDescriptor getConstraint(String name);

   /**
    * Returns all constraint descriptors owned by the table with the given ID as an immutable list.
    *
    * @param tableID
    *           ID of the owning table
    * @return immutable list of constraint descriptors
    */
   Collection<ConstraintDescriptor> getConstraints(int tableID);

   /**
    * Returns the primary key descriptor for the constraint with the given ID.
    *
    * @param primaryKeyID
    *           ID of the primary key descriptor
    * @return primary key descriptor
    */
   PrimaryKeyDescriptor getPrimaryKeyDescriptor(int primaryKeyID);

   /**
    * Checks if a primary key with the given name exists.
    *
    * @param name
    *           primary key name
    * @return {@code true} if the primary key exists, {@code false} otherwise
    */
   boolean hasPrimaryKey(String name);

   /**
    * Returns the primary key descriptor for the constraint with the given name.
    *
    * @param name
    *           name of the primary key
    * @return primary key descriptor
    */
   PrimaryKeyDescriptor getPrimaryKey(String name);

   /**
    * Returns the primary key descriptor that is owned by the table with the given ID.
    *
    * @param tableID
    *           ID of the owning table
    * @return primary key descriptors
    */
   PrimaryKeyDescriptor getPrimaryKey(int tableID);

   /**
    * Returns the primary key descriptor that is owned by the table with the given ID and that are defined as
    * column constraint of the column with the given ID.
    *
    * @param tableID
    *           ID of the owning table
    * @param columnID
    *           ID of the constrained column
    * @return foreign key descriptors
    */
   PrimaryKeyDescriptor getPrimaryKey(int tableID, int columnID);

   /**
    * Returns the foreign key descriptor for the constraint with the given ID.
    *
    * @param foreignKeyID
    *           ID of the foreign key descriptor
    * @return foreign key descriptor
    */
   ForeignKeyDescriptor getForeignKeyDescriptor(int foreignKeyID);

   /**
    * Checks if a foreign key with the given name exists.
    *
    * @param name
    *           foreign key name
    * @return {@code true} if the foreign key exists, {@code false} otherwise
    */
   boolean hasForeignKey(String name);

   /**
    * Returns the foreign key descriptor for the constraint with the given name.
    *
    * @param name
    *           name of the foreign key
    * @return foreign key descriptor
    */
   ForeignKeyDescriptor getForeignKey(String name);

   /**
    * Returns an immutable list of all foreign key descriptors that are owned by the table with the given ID.
    *
    * @param tableID
    *           ID of the owning table
    * @return foreign key descriptors
    */
   Collection<ForeignKeyDescriptor> getForeignKeys(int tableID);

   /**
    * Returns an immutable list of all foreign key descriptors that are owned by the table with the given ID
    * and that are defined as column constraints of the column with the given ID.
    *
    * @param tableID
    *           ID of the owning table
    * @param columnID
    *           ID of the constrained column
    * @return foreign key descriptors
    */
   Collection<ForeignKeyDescriptor> getForeignKeys(int tableID, int columnID);

   /**
    * Returns a series of SQL data definition statements that recreate the contents of this system catalog.
    *
    * @return SQL data definition statements
    */
   String toSQL();
}

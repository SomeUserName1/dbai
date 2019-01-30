/*
 * @(#)ReferenceTable.java   1.0   Jun 10, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import minibase.catalog.DataType;
import minibase.catalog.SystemCatalog;
import minibase.catalog.TableDescriptor;
import minibase.query.QueryException;
import minibase.query.optimizer.Expression;

/**
 * Reference table to resolve symbols during query processing. The reference table is organized as a hierarchy
 * of scopes with the system catalog of Minibase as a global scope.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class ReferenceTable {

   /** Counter to assign unique ids to references. */
   private final AtomicInteger counter;

   /** System catalog used by this reference table. */
   private final SystemCatalog catalog;

   /** Tables in this scope. */
   private final Map<String, TableReference> tables;

   /** Derived (global) columns in this scope. */
   private final Map<String, ColumnReference> columns;

   /** Parent scope of this scope. */
   private final ReferenceTable parent;

   /**
    * Creates a new reference table that is based on the given system catalog.
    *
    * @param catalog
    *           system catalog
    */
   public ReferenceTable(final SystemCatalog catalog) {
      this(catalog, null, new AtomicInteger(0));
   }

   /**
    * Creates a new reference table that is based on the given system catalog and has the given reference
    * table as a parent scope.
    *
    * @param catalog
    *           system catalog
    * @param parent
    *           parent scope
    * @param counter
    *           global counter
    */
   private ReferenceTable(final SystemCatalog catalog, final ReferenceTable parent,
         final AtomicInteger counter) {
      this.catalog = catalog;
      this.parent = parent;
      this.counter = counter;
      this.tables = new HashMap<>();
      this.columns = new HashMap<>();
   }

   /**
    * Returns the next unique reference id.
    *
    * @return reference id
    */
   private int getNextID() {
      return this.counter.getAndIncrement();
   }

   /**
    * Inserts a stored table that is identified by its name in the system catalog into this scope.
    *
    * @param name
    *           table name
    * @throws QueryException
    *            if the table to be inserted is unknown by the catalog
    * @return table reference
    */
   public StoredTableReference insertTable(final String name) throws QueryException {
      return this.insertTable(name, (String) null);
   }

   /**
    * Inserts a stored table that is identified by its name in the system catalog into this scope under the
    * given alias.
    *
    * @param name
    *           table name
    * @param alias
    *           table alias
    * @return table reference
    * @throws QueryException
    *            if the table to be inserted is unknown by the catalog
    */
   public StoredTableReference insertTable(final String name, final String alias) throws QueryException {
      if (!this.catalog.hasTable(name)) {
         throw new QueryException("Unknown table " + name + ".");
      }
      final TableDescriptor descriptor = this.catalog.getTable(name);
      final StoredTableReference table = new StoredTableReference(this.getNextID(), descriptor, alias);
      this.insert(alias != null ? alias : name, table);
      return table;
   }

   /**
    * Inserts a derived table that is identified by the given name and has the given columns into this scope.
    *
    * @param name
    *           table name
    * @param columns
    *           table columns
    * @return table reference
    */
   public DerivedTableReference insertTable(final String name, final List<ColumnReference> columns) {
      final List<ColumnReference> tableColumns = new ArrayList<>();
      for (final ColumnReference column : columns) {
         final ColumnReference tableColumn;
         // Columns of an inner query are exposed to the outer query should no longer be marked as aggregated
         // columns as this will interfere with the correctness check for the outer table expression.
         if (column instanceof DerivedColumnReference) {
            final DerivedColumnReference derivedColumn = (DerivedColumnReference) column;
            if (derivedColumn.isAggregation()) {
               if (column instanceof VirtualColumnReference) {
                  final VirtualColumnReference virtualColumn = (VirtualColumnReference) column;
                  tableColumn = this.insertColumn(virtualColumn.getName(), virtualColumn.getType(),
                        virtualColumn.getSize(), virtualColumn.getInputColumns(), false);
               } else if (column instanceof ExpressionColumnReference
                     || column instanceof AliasedColumnReference) {
                  tableColumn = this.insertColumn(column.getName(), column.getType(), column.getSize(),
                        Collections.singletonList(column), false);
               } else {
                  throw new IllegalStateException(
                        "Column type " + column.getClass().getSimpleName() + " not supported.");
               }
            } else {
               tableColumn = column;
            }
         } else {
            tableColumn = column;
         }
         tableColumns.add(tableColumn);
      }
      final DerivedTableReference table = new DerivedTableReference(this.getNextID(), name, tableColumns);
      this.insert(name, table);
      return table;
   }

   /**
    * Inserts the given table reference into this scope under the given name.
    *
    * @param name
    *           table name
    * @param table
    *           table reference
    */
   private void insert(final String name, final TableReference table) {
      if (this.tables.containsKey(name)) {
         throw new IllegalStateException("Table " + name + " already exists in this scope.");
      }
      this.tables.put(name, table);
   }

   /**
    * Resolves the given table name to a table reference. If the table is not found in the current scope its
    * parent scopes are checked (recursively).
    *
    * @param name
    *           table name
    * @return table reference
    */
   public TableReference resolveTable(final String name) {
      if (this.tables.containsKey(name)) {
         return this.tables.get(name);
      }
      if (this.parent != null) {
         return this.parent.resolveTable(name);
      }
      throw new IllegalStateException("Reference to table " + name + " is invalid.");
   }

   /**
    * Inserts a virtual derived column that is identified by the given name and derived from the given input
    * column into this scope.
    *
    * @param name
    *           column name
    * @param type
    *           declared data type of this reference
    * @param size
    *           declared size of this reference
    * @param inputs
    *           input columns of this reference
    * @param aggregation
    *           indicates whether this column is derived by aggregation
    * @return column reference
    */
   public DerivedColumnReference insertColumn(final String name, final DataType type, final int size,
         final List<ColumnReference> inputs, final boolean aggregation) {
      final VirtualColumnReference column = new VirtualColumnReference(this.getNextID(), name, type, size,
            (double) size / this.catalog.getPageSize(), inputs, aggregation);
      this.insert(name, column);
      return column;
   }

   /**
    * Inserts an aliased column that is identified by the given name for the given original column.
    *
    * @param name
    *           column name
    * @param original
    *           original column
    * @return column reference
    */
   public DerivedColumnReference insertColumn(final String name, final ColumnReference original) {
      final AliasedColumnReference column = new AliasedColumnReference(name, original);
      this.insert(name, column);
      return column;
   }

   /**
    * Inserts an expression column that is identified by the given name and computed using the given
    * expression into this scope.
    *
    * @param name
    *           column name
    * @param expression
    *           column expression
    * @return column reference
    */
   public DerivedColumnReference insertColumn(final String name, final Expression expression) {
      final ExpressionColumnReference column = new ExpressionColumnReference(this.getNextID(), name,
            expression, this.catalog);
      this.insert(name, column);
      return column;
   }

   /**
    * Inserts the given column reference into this scope under the given name.
    *
    * @param name
    *           column name
    * @param column
    *           column reference
    */
   private void insert(final String name, final ColumnReference column) {
      if (this.columns.containsKey(name)) {
         throw new IllegalStateException("Column " + name + " already exists in this scope.");
      }
      this.columns.put(name, column);
   }

   /**
    * Resolves the given column name to a column reference. As no scope name is provided, global derived
    * columns are checked first. Then, all tables in the scope are checked whether they have a column with the
    * given name. If this check returns more than one column, the given column name is rejected as ambiguous.
    * Otherwise the corresponding column reference is returned. If the column is not present in the current
    * scope, its parent scopes are checked (recursively).
    *
    * @param name
    *           column name
    * @return column reference, or {@link Optional#empty()} if the column name is invalid or ambiguous
    */
   public Optional<ColumnReference> resolveColumn(final String name) {
      // Check if it is a derived (global) column.
      if (this.columns.containsKey(name)) {
         return Optional.of(this.columns.get(name));
      }
      // Check if the column is to be found anywhere in the system catalog.
      final List<TableReference> tables = this.tables.values().stream()
            .filter(t -> t instanceof StoredTableReference).filter(st -> this.catalog
                  .hasColumn(((StoredTableReference) st).getDescriptor().getCatalogID(), name))
            .collect(Collectors.<TableReference> toList());
      if (tables.size() == 1) {
         // exactly one match, must be column name
         return this.resolveColumn(tables.get(0).getName(), name);
      }
      if (this.parent != null) {
         return this.parent.resolveColumn(name);
      }
      // TODO how to report difference of invalid/ambiguous?
      return Optional.empty();
      // throw new IllegalStateException(
      // "Reference to column " + name + " is " + (tables.size() < 1 ? "invalid." : "ambiguous."));
   }

   /**
    * Resolves the given scope and column name to a column reference. If the column is not found in the
    * current scope, its parent scopes are checked (recursively).
    *
    * @param scope
    *           scope name
    * @param name
    *           column name
    * @return column reference, or {@link Optional#empty()} if the column name is ultimately invalid
    */
   public Optional<ColumnReference> resolveColumn(final String scope, final String name) {
      if (this.tables.containsKey(scope)) {
         // The table is visible in this scope.
         final TableReference table = this.tables.get(scope);
         return table.getColumn(name);
      }
      if (this.parent != null) {
         return this.parent.resolveColumn(scope, name);
      }
      return Optional.empty();
   }

   /**
    * Opens and returns a new scope.
    *
    * @return new scope
    */
   public ReferenceTable openScope() {
      return new ReferenceTable(this.catalog, this, this.counter);
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append(ReferenceTable.class.getSimpleName());
      result.append("[tables=");
      result.append(this.tables);
      result.append(", columns=");
      result.append(this.columns);
      result.append("]");
      return result.toString();
   }
}

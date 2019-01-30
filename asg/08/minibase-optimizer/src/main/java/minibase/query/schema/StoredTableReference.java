/*
 * @(#)StoredTableReference.java   1.0   Jun 10, 2016
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import minibase.catalog.ColumnDescriptor;
import minibase.catalog.TableDescriptor;

/**
 * A reference to a table in the system catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class StoredTableReference extends AbstractReference implements TableReference {

   /** Table descriptor of this stored table. */
   private final TableDescriptor table;

   /** Optional alias of this stored table. */
   private final Optional<String> alias;

   /** Column cache. */
   private final Map<String, StoredColumnReference> columns;

   /**
    * Creates a new stored table reference to the given table descriptor.
    *
    * @param id
    *           reference id
    * @param table
    *           table descriptor
    */
   StoredTableReference(final int id, final TableDescriptor table) {
      this(id, table, null);
   }

   /**
    * Creates a new stored table reference to the given table descriptor with the given alias.
    *
    * @param id
    *           reference id
    * @param table
    *           table descriptor
    * @param alias
    *           table alias
    */
   StoredTableReference(final int id, final TableDescriptor table, final String alias) {
      super(id);
      this.table = table;
      this.alias = Optional.ofNullable(alias);
      this.columns = new HashMap<>();
   }

   /**
    * Returns the table descriptor of this table.
    *
    * @return table descriptor
    */
   public TableDescriptor getDescriptor() {
      return this.table;
   }

   @Override
   public String getName() {
      return this.alias.orElseGet(() -> this.table.getName());
   }

   @Override
   public Optional<ColumnReference> getColumn(final String name) {
      if (this.columns.containsKey(name)) {
         return Optional.of(this.columns.get(name));
      } else {
         if (!this.table.hasColumn(name)) {
            return Optional.empty();
         }
         final ColumnDescriptor descriptor = this.table.getColumn(name);
         final StoredColumnReference column = new StoredColumnReference(descriptor.getCatalogID(), descriptor,
               this);
         this.columns.put(name, column);
         return Optional.of(column);
      }
   }

   @Override
   public List<ColumnReference> getColumns() {
      final List<ColumnReference> columns = new ArrayList<>();
      for (final ColumnDescriptor descriptor : this.table.getColumns()) {
         StoredColumnReference column = this.columns.get(descriptor.getName());
         if (column == null) {
            column = new StoredColumnReference(descriptor.getCatalogID(), descriptor, this);
            this.columns.put(descriptor.getName(), column);
         }
         columns.add(column);
      }
      return columns;
   }

   @Override
   public int getSize() {
      return this.table.getSize();
   }

   @Override
   public double getWidth() {
      return this.table.getWidth();
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder();
      result.append(this.table.getName());
      result.append(this.columns);
      return result.toString();
   }
}

/*
 * @(#)SchemaSortKey.java   1.0   Mar 5, 2015
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
import java.util.List;
import java.util.Optional;

import minibase.SortKey;
import minibase.catalog.CatalogSortKey;
import minibase.catalog.SortOrder;

/**
 * In the schema for query expressions, a sort key is represented as a list of column references and
 * corresponding list of sort orders.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class SchemaSortKey extends SchemaKey implements SortKey<ColumnReference> {

   /** List of sort orders. */
   private final List<SortOrder> orders;

   /**
    * Constructs a new schema sort key from the given column references and corresponding sort orders.
    *
    * @param columns
    *           key column references
    * @param orders
    *           key column sort orders
    */
   public SchemaSortKey(final List<ColumnReference> columns, final List<SortOrder> orders) {
      super(columns);
      if (columns.size() != orders.size()) {
         throw new IllegalArgumentException("Number of column IDs (" + columns.size()
               + ") does not match number of orders (" + orders.size() + ")");
      }
      this.orders = orders;
   }

   @Override
   public SortOrder getOrder(final int index) {
      return this.orders.get(index);
   }

   @Override
   public boolean isMatch(final List<ColumnReference> columnReferences) {
      if (columnReferences.size() == this.size()) {
         for (int i = 0; i < this.size(); i++) {
            final ColumnReference ref = columnReferences.get(i);
            if (this.getColumn(i).getID() != ref.getID()) {
               return false;
            }
         }
         return true;
      }
      return false;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (this.orders == null ? 0 : this.orders.hashCode());
      return result;
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (this.getClass() != obj.getClass()) {
         return false;
      }
      final SchemaSortKey other = (SchemaSortKey) obj;
      if (this.orders == null) {
         if (other.orders != null) {
            return false;
         }
      } else if (!this.orders.equals(other.orders)) {
         return false;
      }
      return true;
   }

   @Override
   public boolean equals(final List< ? > columns, final List< ? > other) {
      return columns.equals(other);
   }

   /**
    * Tries to resolve the given catalog sort key against the schema that is given in terms of a list of
    * column references.
    *
    * @param key
    *           catalog sort key
    * @param schema
    *           schema column references
    * @return resolved schema sort key
    */
   public static Optional<SchemaSortKey> resolve(final CatalogSortKey key,
         final List<ColumnReference> schema) {
      final List<ColumnReference> columns = new ArrayList<>();
      final List<SortOrder> orders = new ArrayList<>();
      for (int index = 0; index < key.size(); index++) {
         final Integer id = key.getColumn(index);
         for (final ColumnReference column : schema) {
            if (id.intValue() == ((StoredColumnReference) column).getDescriptor().getCatalogID()) {
               columns.add(column);
               orders.add(key.getOrder(index));
            }
         }
      }
      if (key.size() != columns.size()) {
         return Optional.of(new SchemaSortKey(columns, orders));
      }
      return Optional.empty();
   }
}

/*
 * @(#)CatalogSortKey.java   1.0   Feb 20, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.Arrays;
import java.util.List;

import minibase.SortKey;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.StoredColumnReference;

/**
 * Represents the key according to which a table is physically sorted.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class CatalogSortKey extends CatalogKey implements SortKey<Integer> {

   /** Sort order of each sort key column, if the file type is {@link DataOrder#SORTED}. */
   private final SortOrder[] orders;

   /**
    * Constructs a new sort key with the given sort key column identifiers and corresponding sort order.
    *
    * @param columns
    *           column identifiers of the sort key
    * @param orders
    *           sort order of each sort key column
    */
   public CatalogSortKey(final int[] columns, final SortOrder[] orders) {
      super(columns);
      if (columns.length != orders.length) {
         throw new IllegalArgumentException("Number of column IDs (" + columns.length
               + ") does not match number of orders (" + orders.length + ")");
      }
      this.orders = orders;
   }

   /**
    * Returns the sort order of the column with the given index.
    *
    * @param index
    *           sort key column index
    * @return order of key column at given index
    */
   @Override
   public SortOrder getOrder(final int index) {
      return this.orders[index];
   }

   @Override
   public boolean isMatch(final List<ColumnReference> columnReferences) {
      if (columnReferences.size() == this.size()) {
         for (int i = 0; i < this.size(); i++) {
            final ColumnReference ref = columnReferences.get(i);
            if (ref instanceof StoredColumnReference) {
               if (this.getColumn(i).intValue() != ((StoredColumnReference) ref).getDescriptor()
                     .getCatalogID()) {
                  return false;
               }
            } else {
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
      result = prime * result + Arrays.hashCode(this.orders);
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
      final CatalogSortKey other = (CatalogSortKey) obj;
      if (!Arrays.equals(this.orders, other.orders)) {
         return false;
      }
      return true;
   }

   @Override
   public boolean equals(final List< ? > columns, final List< ? > other) {
      return columns.equals(other);
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append("(");
      for (int i = 0; i < this.size(); i++) {
         result.append("[");
         result.append(this.getColumn(i));
         result.append(", ");
         result.append(this.orders[i]);
         result.append("]");
         if (i + 1 < this.size()) {
            result.append(", ");
         }
      }
      result.append(")");
      return result.toString();
   }
}

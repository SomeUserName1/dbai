/*
 * @(#)QuerySortColumn.java   1.0   May 28, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.parser;

import minibase.catalog.SortOrder;

/**
 * Utility class to represent sort columns during parsing..
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class QuerySortColumn {

   /** Column. */
   private final QueryColumn column;

   /** Sort order. */
   private final SortOrder order;

   /**
    * Creates a new sort column with the given sort order.
    *
    * @param column
    *           sort column
    * @param order
    *           sort order
    */
   protected QuerySortColumn(final QueryColumn column, final SortOrder order) {
      this.column = column;
      switch (order) {
         case ASCENDING:
         case DESCENDING:
            this.order = order;
            break;
         default:
            throw new IllegalArgumentException("Sort order " + order + " is not supported.");
      }
   }

   /**
    * Returns the column or this sort column.
    *
    * @return sort column
    */
   public QueryColumn getColumn() {
      return this.column;
   }

   /**
    * Returns the order of this sort column.
    *
    * @return sort order
    */
   public SortOrder getOrder() {
      return this.order;
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder();
      result.append(this.column);
      switch (this.order) {
         case ASCENDING:
            break;
         case DESCENDING:
            result.append(" ");
            result.append("DESC");
            break;
         default:
      }
      return result.toString();
   }
}

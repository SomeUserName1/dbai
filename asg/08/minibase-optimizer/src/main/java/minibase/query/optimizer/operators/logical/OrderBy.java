/*
 * @(#)OrderBy.java   1.0   Jul 21, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.logical;

import minibase.catalog.SortOrder;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;

/**
 * The logical order-by operator orders the result according to a list of sort columns and sort orders.
 * <p>
 * Minibase's query optimizer is based on the Cascades framework for query optimization and, additionally,
 * implements some of the improvements proposed by the Columbia database query optimizer.
 * <ul>
 * <li>Goetz Graefe: <strong>The Cascades Framework for Query Optimization</strong>. In
 * <em>IEEE Data(base) Engineering Bulletin</em>, 18(3), pp. 19-29, 1995.</li>
 * <li>Yongwen Xu: <strong>Efficiency in Columbia Database Query Optimizer</strong>,
 * <em>MSc Thesis, Portland State University</em>, 1998.</li>
 * </ul>
 * The Minibase query optimizer therefore descends from the EXODUS, Volcano, Cascades, and Columbia line of
 * query optimizers, which all use a rule-based, top-down approach to explore the space of possible query
 * execution plans, rather than a bottom-up approach based on dynamic programming.
 * </p>
 *
 * @author Devarsh Karekal Umashankar &lt;devarsh.karekal-umashankar@uni-konstanz.de&gt;
 * @version 1.0
 */
public class OrderBy extends AbstractLogicalOperator {

   /** References to sort columns. */
   private final ColumnReference[] sortColumns;

   /** Sort orders. */
   private final SortOrder[] sortOrders;

   /**
    * Creates a new {@code OrderBy} logical operator with the given sort columns and sort orders.
    *
    * @param sortColumns
    *           references to sort columns
    * @param sortOrders
    *           sort orders
    */
   public OrderBy(final ColumnReference[] sortColumns, final SortOrder[] sortOrders) {
      super(OperatorType.ORDER_BY);
      this.sortColumns = sortColumns;
      this.sortOrders = sortOrders;
   }

   /**
    * Creates a new {@code OrderBy} logical operator. This variant of the constructor is used by the rule
    * engine to create a template of a order-by operator. Therefore, all its fields are initialized to
    * {@code null}.
    */
   public OrderBy() {
      super(OperatorType.ORDER_BY);
      this.sortColumns = null;
      this.sortOrders = null;
   }

   /**
    * Returns the sort columns.
    *
    * @return sort columns
    */
   public ColumnReference[] getSortParams() {
      return this.sortColumns;
   }

   /**
    * Returns the sort orders.
    *
    * @return sort orders
    */
   public SortOrder[] getSortOrders() {
      return this.sortOrders;
   }

   @Override
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      return inputProperties[0];
   }

   @Override
   public Schema getSchema(final Schema... inputSchemas) {
      return inputSchemas[0];
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder(this.getClass().getSimpleName());
      result.append("[");
      for (int i = 0; i < this.sortColumns.length; i++) {
         result.append(this.sortColumns[i]);
         if (SortOrder.ASCENDING.equals(this.sortOrders[i])) {
            result.append(" ASC");
         } else {
            result.append(" DESC");
         }
         if (i + 1 < this.sortColumns.length) {
            result.append(", ");
         }
      }
      result.append("]");
      return result.toString();
   }
}

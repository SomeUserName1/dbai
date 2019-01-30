/*
 * @(#)GetColumn.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.element;

import minibase.query.optimizer.LogicalColumnProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.StoredColumnReference;

/**
 * The {@code GetColumn} element operator accesses values of a single column, for example
 * {@code Sailors.sname}.
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
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class GetColumn extends AbstractConstantOperator {

   /** Reference to the column accessed by this {@code GetColumn} element operator. */
   private final StoredColumnReference column;

   /** Logical properties of this {@code GetColumn} element operator. */
   private final LogicalProperties logicalProperties;

   /**
    * Constructs a new {@code GetColumn} element operator for the given column reference.
    *
    * @param column
    *           column reference
    */
   public GetColumn(final StoredColumnReference column) {
      super(OperatorType.GETCOLUMN);
      this.column = column;
      this.logicalProperties = new LogicalColumnProperties(column.getDescriptor().getStatistics(), column);
   }

   /**
    * Returns the reference to the column that this element operator accesses.
    *
    * @return column reference
    */
   public ColumnReference getColumn() {
      return this.column;
   }

   @Override
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      return this.logicalProperties;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      result.append(this.column.getName());
      result.append("]");
      return result.toString();
   }
}

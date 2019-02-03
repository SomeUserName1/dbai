/*
 * @(#)AbstractPhysicalJoin.java   1.0   Jun 13, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import java.util.Collections;
import java.util.List;

import minibase.query.optimizer.operators.OperatorType;
import minibase.query.schema.ColumnReference;

/**
 * Abstract common superclass of physical join operators.
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
public abstract class AbstractPhysicalJoin extends AbstractPhysicalOperator {

   /** Reference IDs of left columns. */
   private final List<ColumnReference> leftColumns;

   /** Reference IDs of right columns. */
   private final List<ColumnReference> rightColumns;

   /**
    * Creates a new generic join physical operator. The join predicate is given in the form of two sets of the
    * same size. Each set contains a number of references to columns in the system catalog. Columns are
    * assumed to be pair-wise equal, i.e., the join predicate is {@code leftColumns[1] = rightColumns[1] AND}
    * ... {@code AND leftColumns[n] = rightColumns[n]}. If both sets of column reference IDs are empty, a
    * natural join or a cross-product is performed.
    *
    * @param type
    *           join operator type
    * @param leftColumns
    *           left columns
    * @param rightColumns
    *           right columns
    */
   protected AbstractPhysicalJoin(final OperatorType type, final List<ColumnReference> leftColumns,
         final List<ColumnReference> rightColumns) {
      super(type);
      this.leftColumns = leftColumns;
      this.rightColumns = rightColumns;
   }

   /**
    * Constructs a new generic join physical operator. This variant of the constructor is used by the rule
    * engine to create a template of this operator. Therefore, all its fields are initialized to {@code null}.
    *
    * @param type
    *           join operator type
    */
   protected AbstractPhysicalJoin(final OperatorType type) {
      this(type, null, null);
   }

   /**
    * Returns an unmodifiable list containing the references to the join columns of the left input.
    *
    * @return join column references
    */
   public List<ColumnReference> getLeftColumns() {
      return Collections.unmodifiableList(this.leftColumns);
   }

   /**
    * Returns an unmodifiable list containing the references to the join columns of the right input.
    *
    * @return join column references
    */
   public List<ColumnReference> getRightColumns() {
      return Collections.unmodifiableList(this.rightColumns);
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      for (int i = 0; i < this.leftColumns.size(); i++) {
         result.append(this.leftColumns.get(i).getName());
         result.append("=");
         result.append(this.rightColumns.get(i).getName());
         if (i + 1 < this.leftColumns.size()) {
            result.append(" AND ");
         }
      }
      result.append("]");
      return result.toString();
   }
}

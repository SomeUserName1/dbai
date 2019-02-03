/*
 * @(#)LogicalElementProperties.java   1.0   Jun 6, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.ArrayList;
import java.util.List;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.DataType;
import minibase.catalog.Statistics;
import minibase.query.schema.ColumnReference;

/**
 * Logical properties to describe the (collection element) result of an element operator.
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
public class LogicalElementProperties extends LogicalProperties {

   /** Data type of the described collection element. */
   private final DataType type;

   /** Declared size in bytes of the described collection element. */
   private final int size;

   /** Selectivity of the described collection element. */
   private final double selectivity;

   /** Indicates if the described collection element is a constant. */
   private final boolean isConstant;

   /** Input columns of the operator that produces the described collection element. */
   private final List<ColumnReference> inputs;

   /**
    * Constructs new properties to describe an collection element.
    *
    * @param statistics
    *           statistics of the described collection element
    * @param type
    *           data type of the described collection element
    * @param size
    *           declared size in bytes of the described collection element
    * @param selectivity
    *           selectivity of the described collection element
    * @param inputs
    *           input columns of the operator that produces the described collection element
    */
   public LogicalElementProperties(final ColumnStatistics statistics, final DataType type, final int size,
         final double selectivity, final List<ColumnReference> inputs) {
      this(statistics, type, size, selectivity, false, inputs);
   }

   /**
    * Constructs new properties to describe an collection element.
    *
    * @param statistics
    *           statistics of the described collection element
    * @param type
    *           data type of the described collection element
    * @param size
    *           declared size in bytes of the described collection element
    * @param selectivity
    *           selectivity of the described collection element
    * @param isConstant
    *           {@code true} if described collection element is a constant
    * @param inputs
    *           input columns of the operator that produces the described collection element
    */
   public LogicalElementProperties(final ColumnStatistics statistics, final DataType type, final int size,
         final double selectivity, final boolean isConstant, final List<ColumnReference> inputs) {
      super(statistics);
      this.type = type;
      this.size = size;
      this.selectivity = selectivity;
      this.isConstant = isConstant;
      this.inputs = inputs;
   }

   @Override
   protected ColumnStatistics getStatistics() {
      return (ColumnStatistics) super.getStatistics();
   }

   /**
    * Returns the data type of the described collection element.
    *
    * @return data type
    */
   public DataType getType() {
      return this.type;
   }

   /**
    * Returns the declared size in bytes of the described collection element.
    *
    * @return size in bytes
    */
   public int getSize() {
      return this.size;
   }

   /**
    * Returns the selectivity of the described collection element.
    *
    * @return selectivity
    */
   public double getSelectivity() {
      return this.selectivity;
   }

   /**
    * Returns the input columns of the operator that produces the described collection element.
    *
    * @return input columns
    */
   public List<ColumnReference> getInputColumns() {
      return this.inputs;
   }

   /**
    * Returns whether the described collection element is a constant.
    *
    * @return {@code true} if described collection element is a constant, {@code false} otherwise
    */
   public boolean isConstant() {
      return this.isConstant;
   }

   /** Static reference to default properties. */
   private static final LogicalElementProperties DEFAULT_PROPERTIES = new LogicalElementProperties(
         new ColumnStatistics(Statistics.UNKNOWN, Statistics.UNKNOWN, null, null, 0.0), null, 0,
         Statistics.UNKNOWN, new ArrayList<>());

   /**
    * Returns the default logical element properties, where maximum, minimum, and cardinality are set to
    * {@code -1}, selectivity is set to {@code 0}, and the list of input columns is an empty list.
    *
    * @return default logical element properties
    */
   public static LogicalElementProperties defaultProperties() {
      return DEFAULT_PROPERTIES;
   }
}

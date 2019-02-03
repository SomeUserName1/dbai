/*
 * @(#)Constant.java   1.0   Jul 7, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.element;

import minibase.catalog.DataType;
import minibase.query.optimizer.LogicalConstantProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;

/**
 * The constant operator implementation.
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
 * @author Marcel Hanser &lt;marcel.hanser@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class Constant extends AbstractConstantOperator {

   /** Value of the constant. */
   private final Object value;

   /** Data type of the constant value. */
   private final DataType type;

   /** Declared size of the constant value. */
   private final int size;

   /**
    * @param value
    *           value of the constant
    * @param type
    *           data type of the constant
    * @param size
    *           size in bytes of the constant
    */
   public Constant(final Object value, final DataType type, final int size) {
      super(OperatorType.CONSTANT);
      this.value = value;
      this.type = type;
      this.size = size;
   }

   /**
    * Returns the constant value.
    *
    * @return constant value
    */
   public Object getValue() {
      return this.value;
   }

   /**
    * Returns the data type of the constant value.
    *
    * @return constant data type
    */
   public DataType getDataType() {
      return this.type;
   }

   /**
    * Returns the declared size in bytes of the constant value.
    *
    * @return size in bytes
    */
   public int getSize() {
      return this.size;
   }

   @Override
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      return new LogicalConstantProperties(this.value, this.type, this.size);
   }

   @Override
   public String toString() {
      return "Constant(" + this.value.toString() + "::" + this.type.toString() + ")";
   }
}

/*
 * @(#)AbstractOperator.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators;

/**
 * Abstract base class for all operators known to the Minibase query optimizer.
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
public abstract class AbstractOperator implements Operator {

   /** Type of this operator. */
   private final OperatorType type;

   /**
    * Constructs a new operator of the given type.
    *
    * @param type
    *           type of this operator
    */
   protected AbstractOperator(final OperatorType type) {
      this.type = type;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public OperatorType getType() {
      return this.type;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String getName() {
      return this.getClass().getSimpleName();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getArity() {
      return this.type.getArity();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isLogical() {
      return this.type.isLogical();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isPhysical() {
      return this.type.isPhysical();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isLeaf() {
      return this.type.isLeaf();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isElement() {
      return this.type.isElement();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isConstant() {
      return this.type.isConstant();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isCommuting() {
      return false;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int hashCode(final boolean ignoreInputOrder) {
      return this.hashCode();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (this.type == null ? 0 : this.type.ordinal());
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean equals(final Object other, final boolean ignoreInputOrder) {
      return this.equals(other);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (this.getClass() != obj.getClass()) {
         return false;
      }
      final AbstractOperator other = (AbstractOperator) obj;
      if (this.type != other.type) {
         return false;
      }
      return true;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("/");
      result.append(this.getArity());
      return result.toString();
   }
}

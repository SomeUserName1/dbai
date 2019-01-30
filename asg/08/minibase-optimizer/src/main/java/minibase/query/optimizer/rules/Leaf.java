/*
 * @(#)Leaf.java   1.0   May 9, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import minibase.query.optimizer.Group;
import minibase.query.optimizer.operators.AbstractOperator;
import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.operators.OperatorType;

/**
 * A binding operator is a placeholder for a group in a rule.
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
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class Leaf extends AbstractOperator implements Operator {

   /** Identifies this leaf operator within a rule. */
   private final int index;

   /** Identifies the group to which this leaf operator is bound. */
   private final Group group;

   /**
    * Constructs a new leaf operator with the given index.
    *
    * @param index
    *           leaf index
    */
   public Leaf(final int index) {
      this(index, null);
   }

   /**
    * Constructs a new leaf operator with the given index that is bound to the given group.
    *
    * @param index
    *           leaf index
    * @param group
    *           leaf group
    */
   public Leaf(final int index, final Group group) {
      super(OperatorType.LEAF);
      this.index = index;
      this.group = group;
   }

   /**
    * Returns the index of this leaf operator.
    *
    * @return leaf index
    */
   public int getIndex() {
      return this.index;
   }

   /**
    * Returns the group to which this leaf operator is bound or {@code -1} if it is not bound yet.
    *
    * @return leaf group
    */
   public Group getGroup() {
      return this.group;
   }

   @Override
   public int hashCode() {
      throw new UnsupportedOperationException("Leaf#hashCode() is not supported.");
   }

   @Override
   public boolean equals(final Object other) {
      throw new UnsupportedOperationException("Leaf#equals(Object) is not supported.");
   }
}

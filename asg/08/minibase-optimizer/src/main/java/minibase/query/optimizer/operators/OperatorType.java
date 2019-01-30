/*
 * @(#)OperatorType.java   1.0   Jan 3, 2014
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
 * Enumeration of all the logical, physical, and item operators supported by the Minibase query optimizer.
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
public enum OperatorType {

   /** Logical operator <em>get-table</em>. */
   GETTABLE(0, OperatorKind.LOGICAL),
   /** Logical operator <em>select</em>. */
   SELECT(2, OperatorKind.LOGICAL),
   /** Logical operator <em>project</em>. */
   PROJECT(1, OperatorKind.LOGICAL),
   /** Logical operator <em>equijoin</em>. */
   EQJOIN(2, OperatorKind.LOGICAL),
   /** Logical operator <em>distinct</em>. */
   DISTINCT(1, OperatorKind.LOGICAL),
   /** Logical operator <em>aggregate</em>. */
   AGGREGATE(1, OperatorKind.LOGICAL),
   /** Logical operator <em>order by</em>. */
   ORDER_BY(1, OperatorKind.LOGICAL),

   /** Physical operator <em>file-scan</em>. */
   FILESCAN(0, OperatorKind.PHYSICAL),
   /** Physical operator <em>filter</em>, i.e., physical selection. */
   FILTER(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>index-filter</em>, i.e., index-based physical selection. */
   IDXFILTER(1, OperatorKind.PHYSICAL),
   /** Physical operator <em>truncate</em>, i.e., physical projection. */
   TRUNCATE(1, OperatorKind.PHYSICAL),
   /** Physical operator <em>nested-loops-join</em>. */
   NLJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>block-nested-loops-join</em>. */
   BLNLJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>index-nested-loops-join</em>. */
   IDXNLJOIN(1, OperatorKind.PHYSICAL),
   /** Physical operator <em>merge-join</em>. */
   MERGEJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>hash-join</em>. */
   HASHJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>hybrid-hash-join</em>. */
   HHASHJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>bit-index-join</em>. */
   BITMAPIDXJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>g-join</em>. */
   GJOIN(2, OperatorKind.PHYSICAL),
   /** Physical operator <em>hash-duplicates</em>. */
   HASHDUPLICATES(1, OperatorKind.PHYSICAL),
   /** Physical operator <em>hash-aggregate</em>. */
   HASHAGGREGATE(1, OperatorKind.PHYSICAL),
   /** Physical operator <em>sort-aggregate</em>. */
   SORTAGGREGATE(1, OperatorKind.PHYSICAL),
   /** Physical enforcer operator <em>sort</em>. */
   SORT(1, OperatorKind.PHYSICAL),
   /** Element operator <em>compare</em>. */
   COMPARE(-1, OperatorKind.ELEMENT),
   /** Element operator <em>aggregation-function</em>. */
   AGGREGATION_FUNCTION(1, OperatorKind.ELEMENT),
   /** Element operator <em>get-column</em>. */
   GETCOLUMN(0, OperatorKind.ELEMENT | OperatorKind.CONSTANT),
   /** Element operator for <em>constants</em>. */
   CONSTANT(0, OperatorKind.ELEMENT | OperatorKind.CONSTANT),
   /** Leaf operator. */
   LEAF(0, OperatorKind.LEAF);

   /** Arity of this operator. */
   private int arity;
   /** Kind of this operator, i.e., logical, physical, item, etc. */
   private int kind;

   /**
    * Constructs a new operator with the given name and the given arity.
    *
    * @param arity
    *           arity of the operator
    * @param kind
    *           kind of the operator, i.e., logical, physical, item, etc.
    */
   OperatorType(final int arity, final int kind) {
      this.arity = arity;
      this.kind = kind;
   }

   /**
    * Returns the arity of this operator type.
    *
    * @return arity of this operator type
    */
   public int getArity() {
      return this.arity;
   }

   /**
    * Returns {@code true} if this operator type is a logical operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a logical operator, {@code false} otherwise
    */
   public boolean isLogical() {
      return (this.kind & OperatorKind.LOGICAL) > 0;
   }

   /**
    * Returns {@code true} if this operator type is a physical operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a physical operator, {@code false} otherwise
    */
   public boolean isPhysical() {
      return (this.kind & OperatorKind.PHYSICAL) > 0;
   }

   /**
    * Returns {@code true} if this operator type is a leaf operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a leaf operator, {@code false} otherwise
    */
   public boolean isLeaf() {
      return (this.kind & OperatorKind.LEAF) > 0;
   }

   /**
    * Returns {@code true} if this operator type is an element operator and {@code false} otherwise.
    *
    * @return {@code true} if this is an element operator, {@code false} otherwise
    */
   public boolean isElement() {
      return (this.kind & OperatorKind.ELEMENT) > 0;
   }

   /**
    * Returns {@code true} if this operator type is a constant operator and {@code false} otherwise.
    *
    * @return {@code true} if this is a constant operator, {@code false} otherwise
    */
   public boolean isConstant() {
      return (this.kind & OperatorKind.CONSTANT) > 0;
   }

   /**
    * Helper class to distinguish the different kinds of operators type. This is mainly a hack because it is
    * not possible to define these constants in the {@code OperatorType} enumeration before they have to be
    * used.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
    * @version 1.0
    */
   private class OperatorKind {

      /** Constant denoting a logical operator. */
      private static final int LOGICAL = 1;
      /** Constant denoting a physical operator. */
      private static final int PHYSICAL = 2;
      /** Constant denoting an element operator. */
      private static final int ELEMENT = 4;
      /** Constant denoting an constant operator. */
      private static final int CONSTANT = 8;
      /** Constant denoting a leaf operator. */
      private static final int LEAF = 16;
   }
}

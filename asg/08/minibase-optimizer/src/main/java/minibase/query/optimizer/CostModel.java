/*
 * @(#)CostModel.java   1.0   Jun 1, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

/**
 * Enumeration defining the various constants of the cost model that is used by the optimizer.
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
public enum CostModel {

   /** CPU cost of reading one block from the disk. */
   CPU_READ(0.00003),
   /** CPU cost of copying one tuple to the next operator. */
   TOUCH_COPY(0.00001),
   /** CPU cost of evaluating one predicate. */
   CPU_PRED(0.00001),
   /** CPU cost of applying function on one attribute. */
   CPU_APPLY(0.00002),
   /** CPU cost of comparing and moving tuples. */
   CPU_COMP_MOVE(0.00003),
   /** CPU cost of computing a hash function. */
   HASH_COST(0.00002),
   /** CPU cost of finding hash bucket. */
   HASH_PROBE(0.00001),
   /** CPU cost of finding index. */
   INDEX_PROBE(0.00001),
   /** Block factor of table file. */
   BF(100),
   /** Block factor of index file. */
   INDEX_BF(1000),
   /** Block factor of bit index file. */
   BIT_INDEX_BF(10000),
   /** I/O cost of reading one block. */
   IO(0.03),
   /** sequential I/O cost of reading one block. */
   IO_SEQ(0.0075);

   /** Cost value. */
   private final double cost;

   /**
    * Creates a new cost model entry with the given cost.
    *
    * @param cost
    *           cost value
    */
   CostModel(final double cost) {
      this.cost = cost;
   }

   /**
    * Returns the cost value of this cost model entry.
    *
    * @return cost value
    */
   public double getCost() {
      return this.cost;
   }
}

/*
 * @(#)RuleType.java   1.0   May 10, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

/**
 * Defines the types of rules known to the optimizer.
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
public enum RuleType {

   /** EquiJoin(R, S) -> EquiJoin(S, R). */
   EQUI_JOIN_COMMUTE(2),
   /** EquiJoin(EquiJoin(R, S), T) -> EquiJoin(R, EquiJoin(S, T)). */
   EQUI_JOIN_LTOR(3),
   /** EquiJoin(R, EquiJoin(S, T)) -> EquiJoin(EquiJoin(R, S), T). */
   EQUI_JOIN_RTOL(3),
   /** EquiJoin(EquiJoin(R, S), EquiJoin(T, U)) -> EquiJoin(EquiJoin(R, T), EquiJoin(S, U)). */
   EQUI_JOIN_EXCHANGE(4),
   /** Projection[l](Selection(R)) -> Projection[l1](Selection(Projection[l2](R))). **/
   PROJECTION_THROUGH_SELECTION(2),
   /** Projection[l](EquiJoin(R, S)) -> Projection[l1](EquiJoin(Projection[l2](R), Projection[l3](S))). */
   PROJECTION_THROUGH_EQUIJOIN(2),
   /** Selection(EquiJoin(R, S)) -> Selection(EquiJoin(Selection(R), Selection(S))). */
   SELECTION_THROUGH_EQUIJOIN(2),
   /** Aggregation[G, A](EquiJoin(R, S)) -> EquiJoin(R, Aggregation[G, A](S)). */
   AGGREGATION_THROUGH_EQUIJOIN(2),
   /** GetTable(R) -> FileScan(R). */
   GETTABLE_TO_FILESCAN(0),
   /** Selection(R, p) -> Filter(R, p). */
   SELECTION_TO_FILTER(2),
   /** Selection(R, p) -> IndexFilter(R, p). */
   SELECTION_TO_IDXFILTER(1),
   /** Projection[l](R) -> Truncate[l](R). */
   PROJECTION_TO_TRUNCATE(1),
   /** Distinct(R) -> HashDuplicates(R). */
   DISTINCT_TO_HASHDUPLICATES(1),
   /** EquiJoin(R, S) -> NestedLoopsJoin(R, S). */
   EQUI_TO_NESTEDLOOPS_JOIN(2),
   /** EquiJoin(R, S) -> BlockNestedLoopsJoin(R, S). */
   EQUI_TO_BLOCKNESTEDLOOPS_JOIN(2),
   /** EquiJoin(R, S) -> MergeJoin(R, S). */
   EQUI_TO_MERGE_JOIN(2),
   /** EquiJoin(R, S) -> HashJoin(R, S). */
   EQUI_TO_HASH_JOIN(2),
   /** EquiJoin(R, S) -> IndexNestedLoopsJoin(R, I(S)). */
   EQUI_TO_IDXNESTEDLOOPS_JOIN(1),
   /** EquiJoin(R, S) -> BitIndexJoin(R, I(S)). */
   EQUI_TO_BITMAPIDX_JOIN(2),
   /** Aggregation[g, a](R) -> HashAggregation[g, a](R). */
   AGGREGATION_TO_HASH_AGGREGATION(1),
   /** Operator(R) -> Operator(Sort(R)). */
   SORT_ENFORCER(1),
   /** OrderBy[S](R) -> Sort[S](R). */
   ORDER_BY_TO_SORT(1);

   /** Arity of this rule type. */
   private int arity;

   /**
    * Constructs a new rule type with the given name and arity.
    *
    * @param arity
    *           rule type arity
    */
   RuleType(final int arity) {
      this.arity = arity;
   }

   /**
    * Returns the arity of this rule type.
    *
    * @return rule type arity
    */
   public int getArity() {
      return this.arity;
   }
}

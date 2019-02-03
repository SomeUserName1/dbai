/*
 * @(#)Group.java   1.0   Jan 3, 2014
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
import java.util.Collections;
import java.util.List;

import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.operators.logical.LogicalOperator;

/**
 * An expression group contains both a collection of equivalent logical multi-expressions and a collection of
 * logically equivalent physical multi-expressions. Additionally, an expression group captures the logical
 * properties satisfied by all multi-expressions in the group. Finally, an expression group also maintains a
 * circle of winners, which caches the best multi-expressions from previous searches in the expression group
 * together with their respective search context in terms of physical properties. Winners with {@code null}
 * plans are also maintained in the winner's circle to avoid repeated attempts to optimize for the same
 * (unsatisfiable) search context.
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
public class Group {

   /** Optimizer that created this group. */
   private final CascadesQueryOptimizer optimizer;

   /** Unique ID of this group. */
   private final int groupID;

   /** Head of the list of logical multi-expressions contained in this group. */
   private final MultiExpressionList logicalExpressions;

   /** Head of the list of physical multi-expressions contained in this group. */
   private final MultiExpressionList physicalExpressions;

   /** Indicates whether this group is optimized. */
   private boolean optimized;

   /** Indicates whether this group is explored. */
   private boolean explored;

   /** Indicates whether this group is being explored. */
   private boolean exploring;

   /** Indicates whether the group was changed or created. */
   private boolean changed;

   /** Winner's circle. */
   private final List<Winner> winners;

   /** Logical properties of this group. */
   private LogicalProperties logicalProperties;

   /** Lower bound of cost. */
   private Cost lowerBound;

   /** Estimated group size. */
   private double estimatedSize;

   /**
    * Creates a new empty expression group with the given unique ID. In general groups should not be created
    * using this constructor, but rather by invoking method {@link SearchSpace#createGroup()}.
    *
    * @param optimizer
    *           optimizer that creates this group
    * @param groupID
    *           unique ID of this group
    */
   public Group(final CascadesQueryOptimizer optimizer, final int groupID) {
      this.optimizer = optimizer;
      this.groupID = groupID;
      this.logicalExpressions = new MultiExpressionList();
      this.physicalExpressions = new MultiExpressionList();
      this.optimized = false;
      this.explored = false;
      this.exploring = false;
      this.changed = false;
      this.winners = new ArrayList<>();
      this.logicalProperties = null;
      this.estimatedSize = 0.0;
      this.lowerBound = null;
   }

   /**
    * Return the optimizer that created this group.
    *
    * @return query optimizer
    */
   public CascadesQueryOptimizer getOptimizer() {
      return this.optimizer;
   }

   /**
    * Returns the unique ID of this group.
    *
    * @return group ID
    */
   public int getID() {
      return this.groupID;
   }

   /**
    * Returns the first logical expression in this group.
    *
    * @return first logical expression
    */
   public MultiExpression getFirstLogicalExpression() {
      return this.logicalExpressions.getHead();
   }

   /**
    * Returns an unmodifiable list of logical multi-expressions.
    *
    * @return logical multi-expression list
    */
   public List<MultiExpression> getLogicalExpressions() {
      return this.logicalExpressions.asList();
   }

   /**
    * Adds a new physical multi-expression to this group.
    *
    * @param expression
    *           physical multi-expression
    */
   private void addPhysicalExpression(final MultiExpression expression) {
      this.physicalExpressions.add(expression);
   }

   /**
    * Returns the first physical expression in this group.
    *
    * @return first physical expression
    */
   public MultiExpression getFirstPhysicalExpression() {
      return this.physicalExpressions.getHead();
   }

   /**
    * Returns an unmodifiable list of physical multi-expressions.
    *
    * @return physical multi-expression list
    */
   public List<MultiExpression> getPhysicalExpressions() {
      return this.physicalExpressions.asList();
   }

   /**
    * Marks this group as optimized.
    *
    * @param optimized
    *           {@code true} if this group is optimized, {@code false} otherwise
    */
   public void setOptimized(final boolean optimized) {
      this.optimized = optimized;
   }

   /**
    * Checks whether this group is optimized.
    *
    * @return {@code true} if this group is optimized, {@code false} otherwise
    */
   public boolean isOptimized() {
      return this.optimized;
   }

   /**
    * Marks this group as explored.
    *
    * @param explored
    *           {@code true} if this group is explored, {@code false} otherwise
    */
   public void setExplored(final boolean explored) {
      this.explored = explored;
   }

   /**
    * Checks whether this group is explored.
    *
    * @return {@code true} if this group is optimized, {@code false} otherwise
    */
   public boolean isExplored() {
      return this.explored;
   }

   /**
    * Marks this group as being explored.
    *
    * @param exploring
    *           {@code true} if this group is being explored, {@code false} otherwise
    */
   public void setExploring(final boolean exploring) {
      this.exploring = exploring;
   }

   /**
    * Checks whether this group is being explored.
    *
    * @return {@code true} if this group is being explored, {@code false} otherwise
    */
   public boolean isExploring() {
      return this.exploring;
   }

   /**
    * Marks this group as changed.
    *
    * @param changed
    *           {@code true} if this group has changed, {@code false} otherwise
    */
   public void setChanged(final boolean changed) {
      this.changed = changed;
   }

   /**
    * Checks whether this group has changed.
    *
    * @return {@code true} if this group has changed, {@code false} otherwise
    */
   public boolean isChanged() {
      return this.changed;
   }

   /**
    * Returns the logical properties of this expression group.
    *
    * @return logical properties
    */
   public LogicalProperties getLogicalProperties() {
      return this.logicalProperties;
   }

   /**
    * Returns the winners of this group as an unmodifiable list.
    *
    * @return list of winners
    */
   public List<Winner> getWinners() {
      return Collections.unmodifiableList(this.winners);
   }

   /**
    * Returns the lower bound cost of this group.
    *
    * @return lower bound cost
    */
   public Cost getLowerBound() {
      return this.lowerBound;
   }

   /**
    * Returns the estimated size of this group.
    *
    * @return estimated size
    */
   public double getEstimatedSize() {
      return this.estimatedSize;
   }

   /**
    * Adds a new multi-expression to this group.
    *
    * @param expression
    *           multi-expression
    */
   public void addExpression(final MultiExpression expression) {
      // Element and, therefore, constant expressions are treated as logical expressions and
      // added to the corresponding list. A group can contain at most one element or constant
      // expression. Groups containing these types of expressions are final and cannot be
      // altered by transformation or implementation rules.
      if (expression.getOperator().isLogical() || expression.getOperator().isElement()) {
         this.addLogicalExpression(expression);
      } else {
         this.addPhysicalExpression(expression);
      }
   }

   /**
    * Adds a new logical multi-expression to this group.
    *
    * @param expression
    *           logical multi-expression
    */
   private void addLogicalExpression(final MultiExpression expression) {
      // If the group is empty, i.e., if that is the first logical expression to be added,
      // initialize the lower bound and the local properties based on that expression.
      if (this.logicalExpressions.head == null) {
         this.logicalProperties = initLogicalProperties(expression);
         this.estimatedSize = initEstimatedSize(expression);
         if (expression.getOperator().isLogical()) {
            this.lowerBound = this.initLowerBound(expression,
                  (LogicalCollectionProperties) this.logicalProperties);
         } else {
            this.lowerBound = Cost.zero();
         }
      }
      this.logicalExpressions.add(expression);
   }

   /**
    * Adds a winner for the given physical properties and the given cost.
    *
    * @param plan
    *           winning query plan
    * @param physicalProperties
    *           physical properties from search context
    * @param cost
    *           cost from search context
    * @param ready
    *           indicates whether the plan is ready or under construction
    */
   public void addWinner(final MultiExpression plan, final PhysicalProperties physicalProperties,
         final Cost cost, final boolean ready) {
      this.setChanged(true);
      // Check if there is an existing winner for these properties
      final Winner winner = this.getWinner(physicalProperties);
      if (winner != null) {
         this.winners.remove(winner);
      }
      this.winners.add(new Winner(plan, physicalProperties, cost, ready));
   }

   /**
    * Returns the winner that matches the given physical properties or {@code null} if there is no such
    * winner.
    *
    * @param physicalProperties
    *           physical properties
    * @return matching winner or {@code null} if there is none
    */
   public Winner getWinner(final PhysicalProperties physicalProperties) {
      for (final Winner winner : this.winners) {
         if (winner.getPhysicalProperties().equals(physicalProperties)) {
            return winner;
         }
      }
      return null;
   }

   /**
    * Returns the winner status of this group for the given context.
    *
    * @param context
    *           optimizer context
    * @param physicalProperties
    *           physical properties
    * @return winner status
    */
   public WinnerStatus getWinnerStatus(final SearchContext context,
         final PhysicalProperties physicalProperties) {
      final Winner winner = this.getWinner(context.getRequiredProperties());
      if (winner == null) {
         // There is no winner and therefore a new search is needed
         return WinnerStatus.NEWSEARCH;
      }
      if (!winner.isReady()) {
         throw new OptimizerError("Query cannot be recursive.");
      }
      final MultiExpression plan = winner.getPlan();
      final Cost winnerCost = winner.getCost();
      final Cost requiredCost = context.getUpperBound();
      if (requiredCost == null) {
         throw new OptimizerError("Required cost cannot be null.");
      }
      if (plan != null) {
         // There is a non-null winner
         if (requiredCost.compareTo(winnerCost) >= 0) {
            // Real winner with cost less that required cost
            return WinnerStatus.SATISFIED;
         } else {
            // Real winner but search cannot be satisfied as winner's cost is higher than required cost
            return WinnerStatus.UNSATISFIABLE;
         }
      } else {
         if (winnerCost.compareTo(requiredCost) >= 0) {
            // Previous search was unsuccessful and winner's cost is higher than required cost
            return WinnerStatus.UNSATISFIABLE;
         } else {
            // Previous search was unsuccessful but winner's cost is less than required cost
            return WinnerStatus.MORESEARCH;
         }
      }
   }

   /**
    * Estimates the number of tables involved in the given expression.
    *
    * @param expression
    *           multi-expression
    * @return estimated number of tables
    */
   public static int estimateTableCount(final MultiExpression expression) {
      int tableCount = 0;
      for (int i = 0; i < expression.getOperator().getArity(); i++) {
         final Group group = expression.getInput(i);
         final MultiExpression input = group.getFirstLogicalExpression();
         if (OperatorType.EQJOIN.equals(input.getOperator().getType())) {
            tableCount += Group.estimateTableCount(input);
         } else {
            tableCount += 1;
         }
      }
      return tableCount;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + this.groupID;
      return result;
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
      final Group other = (Group) obj;
      if (this.groupID != other.groupID) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getClass().getSimpleName());
      result.append("[");
      result.append(this.groupID);
      result.append("]");
      return result.toString();
   }

   /**
    * Initializes the logical properties of this group based on the given (logical) multi-expression.
    *
    * @param expression
    *           logical multi-expression
    * @return logical properties
    */
   private static LogicalProperties initLogicalProperties(final MultiExpression expression) {
      final int arity = expression.getOperator().getArity();
      final LogicalOperator operator = (LogicalOperator) expression.getOperator();
      if (arity == 0) {
         return operator.getLogicalProperties();
      }
      final LogicalProperties[] inputLogicalProperties = new LogicalProperties[arity];
      for (int i = 0; i < arity; i++) {
         inputLogicalProperties[i] = expression.getInput(i).getLogicalProperties();
      }
      return operator.getLogicalProperties(inputLogicalProperties);
   }

   /**
    * Initializes the estimated size of this group based on the given (logical) multi-expression.
    *
    * @param expression
    *           logical multi-expression
    * @return estimated group size
    */
   private static double initEstimatedSize(final MultiExpression expression) {
      if (OperatorType.EQJOIN.equals(expression.getOperator().getType())) {
         // TODO Magic numbers.
         return Math.pow(2, estimateTableCount(expression)) * 2.5;
      }
      return 0.0;
   }

   /**
    * Initializes the lower bound of this group based on the given (logical) multi-expression and logical
    * properties.
    *
    * @param expression
    *           logical multi-expression
    * @param logicalProperties
    *           logical properties
    * @return lower cost bound
    */
   private Cost initLowerBound(final MultiExpression expression,
         final LogicalCollectionProperties logicalProperties) {
      Cost cost;
      if (OperatorType.GETTABLE.equals(expression.getOperator().getType())) {
         cost = Cost.zero();
      } else {
         cost = Cost.touchCopyCost(logicalProperties);
      }
      if (this.optimizer.getSettings().isColumnUniqueCardinalityPruning()) {
         cost = cost.plus(Cost.fetchingCost(logicalProperties));
      }
      return cost;
   }

   /**
    * Private class implementing a simple head-tail linked list for multi-expressions.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private final class MultiExpressionList {

      /** Head of the linked list. */
      private MultiExpression head;
      /** Tail of the linked list. */
      private MultiExpression tail;

      /**
       * Constructs a new empty linked list.
       */
      private MultiExpressionList() {
         this.head = null;
         this.tail = null;
      }

      /**
       * Returns the first multi-expression in the linked list.
       *
       * @return first multi-expression
       */
      private MultiExpression getHead() {
         return this.head;
      }

      /**
       * Adds a multi-expression to the linked list and updates the head and tail as required.
       *
       * @param expression
       *           multi-expression
       */
      private void add(final MultiExpression expression) {
         if (this.tail == null) {
            this.head = expression;
         } else {
            this.tail.setNext(expression);
         }
         this.tail = expression;
      }

      /**
       * Returns this linked list as an unmodifiable list.
       *
       * @return unmodifiable multi-expression list
       */
      private List<MultiExpression> asList() {
         final List<MultiExpression> result = new ArrayList<>();
         MultiExpression current = this.head;
         while (current != null) {
            result.add(current);
            current = current.getNext();
         }
         return Collections.unmodifiableList(result);
      }
   }
}

/*
 * @(#)SearchSpace.java   1.0   Jan 3, 2014
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

import minibase.query.optimizer.operators.element.ConstantOperator;
import minibase.query.optimizer.operators.physical.PhysicalOperator;
import minibase.query.optimizer.util.HashIndex;
import minibase.query.optimizer.util.PlanExplainer;
import minibase.query.optimizer.util.StrongReference;

/**
 * The search space defines range of alternative execution plans that will be explored for a given query.
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
public final class SearchSpace {

   // TODO Move to {@code CascadesSettings}.
   /** Size of the hash table that is used to detect duplicate/equivalent multi-expressions. */
   private static final int TABLE_SIZE = 8192;

   /** Query optimizer to which this search space belongs. */
   private final CascadesQueryOptimizer optimizer;

   /** List of all groups in the search space. */
   private final List<Group> groups;

   /** Lookup table for multi-expressions. */
   private final HashIndex<MultiExpression> lookupTable;

   /**
    * Constructs a new search space and initializes all internal data structures.
    *
    * @param optimizer
    *           query optimizer to which this search space belongs
    * @param tableSize
    *           size of the hash table to detect duplicate expressions
    */
   public SearchSpace(final CascadesQueryOptimizer optimizer, final int tableSize) {
      // TODO Complete search space implementation
      this.optimizer = optimizer;
      this.groups = new ArrayList<>();
      this.lookupTable = new HashIndex<>(tableSize);
   }

   /**
    * Constructs a new search space and initializes all internal data structures.
    *
    * @param optimizer
    *           query optimizer to which this search space belongs
    */
   public SearchSpace(final CascadesQueryOptimizer optimizer) {
      this(optimizer, TABLE_SIZE);
   }

   /**
    * Returns the optimizer to which this search space belongs.
    *
    * @return query optimizer
    */
   public CascadesQueryOptimizer getOptimizer() {
      return this.optimizer;
   }

   /**
    * Returns the size of the search space in terms of the number of groups it contains.
    *
    * @return search space size
    */
   public int size() {
      return this.groups.size();
   }

   /**
    * Creates and returns a new empty group.
    *
    * @return expression group
    */
   public Group createGroup() {
      final Group group = new Group(this.optimizer, this.groups.size());
      // TODO Initialize group for optimization of all interesting relevant properties (directive IRPROP).
      this.groups.add(group);
      return group;
   }

   /**
    * Returns the group in the search space with the given group ID.
    *
    * @param groupID
    *           group ID
    * @return matching group
    */
   public Group getGroup(final int groupID) {
      if (0 <= groupID && groupID < this.groups.size()) {
         return this.groups.get(groupID);
      }
      throw new IllegalArgumentException("Group ID " + groupID + " does not exist.");
   }

   /**
    * Returns an unmodifiable list of all groups that are currently in the search space.
    *
    * @return expression group list
    */
   public List<Group> getGroups() {
      return Collections.unmodifiableList(this.groups);
   }

   /**
    * Merges the two groups.
    *
    * @param firstGroup
    *           first expression group
    * @param secondGroup
    *           second expression group
    * @return merged group
    */
   private Group mergeGroups(final Group firstGroup, final Group secondGroup) {
      if (firstGroup.getID() > secondGroup.getID()) {
         return secondGroup;
      }
      return firstGroup;
   }

   /**
    * Inserts the given expression into the search space and converts it to a multi-expression.
    *
    * @param expression
    *           expression to insert
    * @return converted multi-expression, or {@code null} if the expression already existed
    */
   public MultiExpression insert(final Expression expression) {
      return this.insert(expression, new StrongReference<Group>());
   }

   /**
    * Inserts the given expression into the search space, converts it to a multi-expression, and assigns it to
    * the given group.
    *
    * @param expression
    *           expression to insert
    * @param group
    *           group into which the expression is inserted
    * @return converted multi-expression, or {@code null} if the expression already existed
    */
   public MultiExpression insert(final Expression expression, final Group group) {
      return this.insert(expression, new StrongReference<>(group));
   }

   /**
    * Inserts the given expression into the search space, converts it to a multi-expression, and assigns it to
    * the given group.
    *
    * @param expression
    *           expression to insert
    * @param groupRef
    *           reference to group into which the expression is inserted
    * @return converted multi-expression, or {@code null} if the expression already existed
    */
   public MultiExpression insert(final Expression expression, final StrongReference<Group> groupRef) {
      MultiExpression result = new MultiExpression(this, expression, groupRef.get());
      // only logical multi-expressions are checked for duplicates, not physical multi-expressions
      MultiExpression duplicate = null;
      if (expression.getOperator().isLogical()) {
         duplicate = this.findDuplicate(result);
         if (groupRef.isNull() && duplicate == null) {
            final MultiExpression equivalent = this.findEquivalent(result);
            if (equivalent != null) {
               groupRef.set(equivalent.getGroup());
            }
         }
      }
      if (duplicate == null) {
         final Group[] inputs = new Group[expression.getSize()];
         for (int i = 0; i < inputs.length; i++) {
            inputs[i] = result.getInput(i);
         }
         // physical multi-expression or no duplicate logical multi-expression found
         if (groupRef.isNull()) {
            // create a new group
            final Group group = this.createGroup();
            result = new MultiExpression(expression.getOperator(), inputs, group);
            group.addExpression(result);
            groupRef.set(group);
         } else {
            // use an existing group
            final Group group = groupRef.get();
            result = new MultiExpression(expression.getOperator(), inputs, group);
            group.addExpression(result);
         }
         // if this is a logical multi-expression, insert it into the lookup table
         if (result.getOperator().isLogical()) {
            final boolean ignoreInputOrder = result.getOperator().isCommuting();
            this.lookupTable.insert(result.hashCode(ignoreInputOrder), result);
         }
         return result;
      } else {
         // existing logical multi-expression
         final Group group = groupRef.get();
         if (group == null) {
            // create a new group
            groupRef.set(duplicate.getGroup());
         } else if (!group.equals(duplicate.getGroup())) {
            // merge with an existing group
            groupRef.set(this.mergeGroups(group, duplicate.getGroup()));
         }
         return null;
      }
   }

   /**
    * Checks whether the given multi-expression is already in the search space and, if so, returns it.
    *
    * @param expression
    *           multi-expression to search for
    * @return duplicate multi-expression or {@code null} if there is no duplicate
    */
   private MultiExpression findDuplicate(final MultiExpression expression) {
      final boolean ignoreInputOrder = expression.getOperator().isCommuting();
      return this.lookupTable.contains(expression.hashCode(ignoreInputOrder), expression);
   }

   /**
    * Checks whether an equivalent multi-expression is already in the search space and, if so, returns it.
    *
    * @param expression
    *           multi-expression to find equivalent for
    * @return equivalent multi-expression or {@code null} if there is no equivalent
    */
   private MultiExpression findEquivalent(final MultiExpression expression) {
      if (expression.getOperator().isCommuting()) {
         return this.lookupTable.contains(expression.hashCode(true), expression,
               (final MultiExpression object, final MultiExpression other) -> object.equals(other, true));
      }
      return null;
   }

   /**
    * Extracts and returns a query expression by traversing the search space starting from the given root
    * group and collecting all the winners that match the given required properties.
    *
    * @param root
    *           root group
    * @param requiredProperties
    *           required physical properties
    * @param explain
    *           {@code true} if the returned expression should contain optimizer metadata, {@code false}
    *           otherwise
    * @return query expression
    */
   public Expression extract(final Group root, final PhysicalProperties requiredProperties,
         final boolean explain) {
      final LogicalProperties properties = root.getLogicalProperties();
      final MultiExpression expression = root.getFirstLogicalExpression();
      if (expression.getOperator().isConstant()) {
         final ConstantOperator constantOperator = (ConstantOperator) expression.getOperator();
         if (explain) {
            return new ExplainedExpression(constantOperator, new ExplainedExpression[0],
                  constantOperator.getCost(), Double.NaN, properties.getUniqueCardinality(), Double.NaN);
         } else {
            return new Expression(constantOperator, new Expression[0]);
         }
      } else {
         final Winner winner = root.getWinner(requiredProperties);
         if (winner != null && winner.isReady() && winner.getPlan() != null) {
            final MultiExpression plan = winner.getPlan();
            if (explain) {
               final ExplainedExpression[] inputs = new ExplainedExpression[plan.getOperator().getArity()];
               for (int i = 0; i < inputs.length; i++) {
                  final PhysicalProperties inputRequiredProperties = getInputRequiredProperties(plan,
                        requiredProperties, i);
                  inputs[i] = (ExplainedExpression) this.extract(plan.getInput(i), inputRequiredProperties,
                        explain);
               }
               if (expression.getOperator().isLogical()) {
                  final LogicalCollectionProperties collectionProperties = (LogicalCollectionProperties) properties;
                  return new ExplainedExpression(plan.getOperator(), inputs, winner.getCost(),
                        collectionProperties.getCardinality(), collectionProperties.getUniqueCardinality(),
                        collectionProperties.getStatistics().getWidth());
               } else {
                  return new ExplainedExpression(plan.getOperator(), inputs, winner.getCost(), Double.NaN,
                        properties.getUniqueCardinality(), Double.NaN);
               }
            } else {
               final Expression[] inputs = new Expression[plan.getOperator().getArity()];
               for (int i = 0; i < inputs.length; i++) {
                  final PhysicalProperties inputRequiredProperties = getInputRequiredProperties(plan,
                        requiredProperties, i);
                  inputs[i] = this.extract(plan.getInput(i), inputRequiredProperties, explain);
               }
               return new Expression(plan.getOperator(), inputs);
            }
         }
      }
      throw new OptimizerError("No winner found for Group [" + root.getID() + "].");
   }

   /**
    * Returns the physical properties that are required of the input with the given input number of the given
    * plan.
    *
    * @param plan
    *           query execution plan
    * @param requiredProperties
    *           required physical properties
    * @param inputNo
    *           input number
    * @return required physical properties of the corresponding input
    */
   private static PhysicalProperties getInputRequiredProperties(final MultiExpression plan,
         final PhysicalProperties requiredProperties, final int inputNo) {
      final StrongReference<PhysicalProperties> inputRequiredProperties = new StrongReference<>();
      final PhysicalOperator physicalOperator = (PhysicalOperator) plan.getOperator();
      final boolean possible = physicalOperator.satisfyRequiredProperties(requiredProperties,
            plan.getInput(inputNo).getLogicalProperties(), inputNo, inputRequiredProperties);
      if (possible) {
         return inputRequiredProperties.get();
      }
      throw new OptimizerError("Plan is not possible: " + plan + ".");
   }

   /**
    * Returns a formatted string that lists information about all groups in the given search space.
    *
    * @return formatted string
    */
   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder(
            "--------------------------------------------------------------\n");
      for (final Group group : this.groups) {
         PlanExplainer.appendGroup(result, "", group);
         result.append("--------------------------------------------------------------\n");
      }
      return result.toString();
   }
}

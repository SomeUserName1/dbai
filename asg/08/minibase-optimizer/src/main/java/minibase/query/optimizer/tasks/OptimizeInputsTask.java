/*
 * @(#)OptimizeInputsTask.java   1.0   May 25, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.tasks;

import minibase.catalog.DataOrder;
import minibase.query.optimizer.CascadesQueryOptimizer;
import minibase.query.optimizer.Cost;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.SearchSpace;
import minibase.query.optimizer.Winner;
import minibase.query.optimizer.WinnerStatus;
import minibase.query.optimizer.operators.element.ConstantOperator;
import minibase.query.optimizer.operators.physical.PhysicalOperator;
import minibase.query.optimizer.util.StrongReference;

/**
 * Task that optimizes the inputs of a multi-expression by determining whether a multi-expression satisfies
 * the current context.
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
public final class OptimizeInputsTask extends AbstractOptimizerTask {

   /** Multi-expression to optimize. */
   private final MultiExpression expression;

   /** Local cost of the expression. */
   private Cost localCost;

   /** Number of the input that is currently being optimized. */
   private int currentInputNo;

   /** Number of the input that was previously optimized. */
   private int previousInputNo;

   /** List of input costs. */
   private Cost[] inputCosts;

   /** List of input logical properties. */
   private LogicalProperties[] inputLogicalProperties;

   /**
    * Creates a new optimizer task to optimize the inputs of the given multi-expression.
    *
    * @param optimizer
    *           Cascades optimizer that scheduled this task
    * @param context
    *           search context used by this task
    * @param parent
    *           parent task
    * @param last
    *           {@code true} if this is the last task for this group, {@code false} otherwise
    * @param bound
    *           bound for global epsilon pruning, {@code null} if not global epsilon pruning is to be
    *           performed
    * @param expression
    *           expression to optimize
    */
   public OptimizeInputsTask(final CascadesQueryOptimizer optimizer, final SearchContext context,
         final OptimizerTask parent, final boolean last, final Cost bound, final MultiExpression expression) {
      super(optimizer, context, parent, false, last, bound);
      if (!expression.getOperator().isPhysical() && !expression.getOperator().isElement()) {
         throw new IllegalStateException("Cost can only be computed for physical and element operators.");
      }
      this.expression = expression;
      this.localCost = null;
      this.currentInputNo = -1;
      this.previousInputNo = -1;
      this.inputCosts = null;
      this.inputLogicalProperties = null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void perform(final SearchSpace searchSpace) {
      // Assert that the expression is a physical expression
      if (!this.expression.getOperator().isPhysical() && !this.expression.getOperator().isElement()) {
         throw new IllegalStateException("Physical or element expression expected.");
      }
      final PhysicalOperator operator = (PhysicalOperator) this.expression.getOperator();
      final Group group = this.expression.getGroup();
      final Cost upperBound = this.getSearchContext().getUpperBound();
      final PhysicalProperties requiredProperties = this.getSearchContext().getRequiredProperties();

      // If global epsilon pruning happened, terminate this task
      if (this.getOptimizer().getSettings().isGlobalEpsilonPruning() && this.getSearchContext().isFinished()) {
         if (this.isLast()) {
            // Since this is the last task for the group it is marked as optimized
            group.setOptimized(true);
         }
         return;
      }

      // Since this is the first execution of perform some members of this class need to be initialized.
      if (this.currentInputNo == -1) {
         // Initialize input logical properties
         this.inputLogicalProperties = new LogicalProperties[this.getArity()];
         for (int i = 0; i < this.getArity(); i++) {
            final Group inputGroup = this.expression.getInput(i);
            this.inputLogicalProperties[i] = inputGroup.getLogicalProperties();
         }
         // Compute the local operator cost of the multi-expression being optimized in the given group
         this.localCost = operator.getLocalCost(group.getLogicalProperties(), this.inputLogicalProperties);
         // Initialize the costs for all input groups of the expression
         this.inputCosts = this.initInputCosts(this.expression, this.inputLogicalProperties,
               this.getSearchContext());
         if (this.inputCosts == null) {
            this.updateGroup(group, requiredProperties);
            return;
         }
         // Make sure the initialization code only runs once
         this.currentInputNo++;
      }
      // Set current cost
      Cost currentCost = Cost.totalCost(this.localCost, this.inputCosts);
      if (this.getOptimizer().getSettings().isPruning() && currentCost.compareTo(upperBound) >= 0) {
         // If pruning is enabled and the current cost is higher than the upper bound of the optimizer
         // context, terminate this task
         this.updateGroup(group, requiredProperties);
         return;
      }
      // Calculate cost for remaining inputs
      for (int i = this.currentInputNo; i < this.getArity(); i++) {
         final Group inputGroup = this.expression.getInput(i);
         // Special case: the input group is a constant group, i.e., uses an element operator
         if (inputGroup.getFirstLogicalExpression().getOperator().isConstant()) {
            continue;
         }
         // Determine property required of the input that is currently optimized
         final StrongReference<PhysicalProperties> physicalPropertiesRef = new StrongReference<>();
         final boolean satisfiable = operator.satisfyRequiredProperties(requiredProperties,
               this.inputLogicalProperties[i], i, physicalPropertiesRef);
         PhysicalProperties requiredInputProperties = physicalPropertiesRef.get();
         if (this.getOptimizer().getSettings().isPruning() && !satisfiable) {
            throw new IllegalStateException("If pruning is enabled, input properties should be satisfiable.");
         }
         if (!satisfiable) {
            this.updateGroup(group, requiredProperties);
            return;
         }
         if (requiredInputProperties == null) {
            requiredInputProperties = PhysicalProperties.anyPhysicalProperties();
         }
         final SearchContext inputGroupContext = new SearchContext(requiredInputProperties, Cost.infinity());
         final WinnerStatus status = inputGroup.getWinnerStatus(inputGroupContext, null);
         if (WinnerStatus.UNSATISFIABLE.equals(status)) {
            // Input group is unsatisfiable
            this.updateGroup(group, requiredProperties);
            return;
         } else if (WinnerStatus.SATISFIED.equals(status)) {
            // Input group contains a winner with a non-null plan from which cost can be used
            final Winner winner = inputGroup.getWinner(requiredInputProperties);
            if (!winner.isReady()) {
               throw new IllegalStateException("Winner is not ready.");
            }
            this.inputCosts[i] = winner.getCost();
            // TODO This piece of code can be factored out.
            // Update current cost
            currentCost = Cost.totalCost(this.localCost, this.inputCosts);
            if (this.getOptimizer().getSettings().isPruning() && currentCost.compareTo(upperBound) >= 0) {
               // If pruning is enabled and the current cost is higher than the upper bound of the optimizer
               // context, terminate this task
               this.updateGroup(group, requiredProperties);
               return;
            }
         } else if (i != this.previousInputNo) {
            // Winner status is either WinnerStatus.NEWSEARCH or WinnerStatus.MORESEARCH, i.e., there is no
            // winner and optimize group was not just performed before.

            // Adjust currentInputNo and previousInputNo to track progress after returning from self-push
            this.currentInputNo = i;
            this.previousInputNo = i;

            // Self push
            this.getOptimizer().addOptimizerTasks(this);

            // Build a context for the input group task. First calculate upper bound for search in input
            // group. Upper bounds are irrelevant unless pruning is enabled.
            Cost inputUpperBound = upperBound;
            if (this.getOptimizer().getSettings().isPruning()) {
               currentCost = Cost.totalCost(this.localCost, this.inputCosts);
               // Subtract the current cost
               inputUpperBound = inputUpperBound.minus(currentCost);
               // Add contribution of the current input group
               inputUpperBound = inputUpperBound.plus(this.inputCosts[i]);
            }
            final SearchContext inputContext = new SearchContext(requiredInputProperties, inputUpperBound);
            Cost epsilonBound = null;
            if (this.getOptimizer().getSettings().isGlobalEpsilonPruning()) {
               epsilonBound = Cost.zero();
               if (this.getBound().compareTo(this.localCost) > 0) {
                  // Calculate new epsilon bound
                  epsilonBound = this.getBound().minus(this.localCost);
                  if (this.getArity() > 0) {
                     epsilonBound = epsilonBound.divide(this.getArity());
                  }
               }
            }
            this.getOptimizer().addOptimizerTasks(
                  new OptimizeGroupTask(this.getOptimizer(), inputContext, this, true, epsilonBound,
                        inputGroup));
            return;
         } else {
            // Since i == previousInputNo, the optimizer just returned from an optimize group on inputGroup
            // and, therefore, it is impossible to plan for this context
            this.updateGroup(group, requiredProperties);
            return;
         }
      }

      // If arity is zero, it needs to be ensured that the expression can satisfy the required properties.
      if (this.getArity() == 0 && !DataOrder.ANY.equals(requiredProperties.getOrder())) {
         final PhysicalProperties outputPhysicalProperties = operator.getPhysicalProperties();
         if (!requiredProperties.equals(outputPhysicalProperties)) {
            this.updateGroup(group, requiredProperties);
            return;
         }
      }
      // TODO Trace when the first complete plan is costed (directive FIRSTPLAN).

      // Update current cost
      currentCost = Cost.totalCost(this.localCost, this.inputCosts);
      // If global epsilon pruning is on, there might be an easy winner
      if (this.getOptimizer().getSettings().isGlobalEpsilonPruning()) {
         // Search has found a final winner
         if (this.getBound().compareTo(currentCost) >= 0) {
            group.addWinner(this.expression, requiredProperties, currentCost, true);
            this.getSearchContext().setUpperBound(currentCost);
            this.getSearchContext().setFinished(true);
            this.updateGroup(group, requiredProperties);
            return;
         }
      }

      if (this.getOptimizer().getSettings().isHalting()) {
         // TODO Implement optional optimizer halting for certain conditions.
         throw new UnsupportedOperationException("Optimizer halting is not yet implemented.");
      }

      // Check that the winner satisfies the current optimizer context
      if (currentCost.compareTo(upperBound) >= 0) {
         this.updateGroup(group, requiredProperties);
         return;
      }

      // TODO Optimize group for all interesting relevant properties (directive IRPROP).
      final Winner localWinner = group.getWinner(requiredProperties);
      if (localWinner.getPlan() != null && currentCost.compareTo(localWinner.getCost()) >= 0) {
         // There is already a non-null local winner and the current expression is more expensive
         this.updateGroup(group, requiredProperties);
         return;
      } else {
         // The expression that is currently being optimized is a winner.
         group.addWinner(this.expression, requiredProperties, currentCost, this.isLast());
         // Update the upper bound of the current context
         this.getSearchContext().setUpperBound(currentCost);
         this.updateGroup(group, requiredProperties);
         return;
      }
   }

   /**
    * If this task is still the last optimizer task for this group, the group will be marked as completed in
    * terms of optimization.
    *
    * @param group
    *           expression group
    * @param requiredProperties
    *           required physical properties
    */
   private void updateGroup(final Group group, final PhysicalProperties requiredProperties) {
      if (this.isLast()) {
         // TODO Optimize group for all interesting relevant properties (directive IRPROP).
         final Winner winner = group.getWinner(requiredProperties);
         winner.setReady(true);
         this.expression.getGroup().setOptimized(true);
      }
   }

   /**
    * Initializes the costs of each input to the give expression.
    *
    * @param expression
    *           multi-expression
    * @param inputLogicalProperties
    *           input logical properties
    * @param context
    *           optimizer context
    * @return list of input costs
    */
   private Cost[] initInputCosts(final MultiExpression expression,
         final LogicalProperties[] inputLogicalProperties, final SearchContext context) {
      final PhysicalOperator operator = (PhysicalOperator) expression.getOperator();
      final int arity = operator.getArity();
      final Cost[] result = new Cost[arity];
      // Initialize cost for all input groups
      for (int i = 0; i < arity; i++) {
         // If not pruning is applied, initial cost values are zero
         if (!this.getOptimizer().getSettings().isPruning()) {
            if (this.getOptimizer().getSettings().isColumnUniqueCardinalityPruning()) {
               throw new IllegalStateException(
                     "Column unique cardinality pruning cannot be enable if pruning is disabled.");
            }
            result[i] = Cost.zero();
            continue;
         }
         // Get the group that corresponds to the current input
         final Group inputGroup = expression.getInput(i);
         // Special case: the input group is a constant group, i.e., uses an element operator
         if (inputGroup.getFirstLogicalExpression().getOperator().isConstant()) {
            result[i] = ((ConstantOperator) inputGroup.getFirstLogicalExpression().getOperator()).getCost();
            continue;
         }
         // Determine property required of the input that is currently initialized
         final StrongReference<PhysicalProperties> physicalPropertiesRef = new StrongReference<>();
         final boolean satisfiable = operator.satisfyRequiredProperties(context.getRequiredProperties(),
               inputLogicalProperties[i], i, physicalPropertiesRef);
         PhysicalProperties requiredInputProperties = physicalPropertiesRef.get();
         if (!satisfiable) {
            // Required properties cannot be satisfied for this input
            return null;
         }
         if (requiredInputProperties == null) {
            requiredInputProperties = PhysicalProperties.anyPhysicalProperties();
         }
         // TODO Optimize group for all interesting relevant properties (directive IRPROP).
         final SearchContext inputGroupContext = new SearchContext(requiredInputProperties, Cost.infinity());
         final WinnerStatus status = inputGroup.getWinnerStatus(inputGroupContext, null);
         if (WinnerStatus.UNSATISFIABLE.equals(status)) {
            // Input group is unsatisfiable
            return null;
         } else if (WinnerStatus.SATISFIED.equals(status)) {
            // Input group is satisfied, get the cost of its winner
            final Winner winner = inputGroup.getWinner(requiredInputProperties);
            if (!winner.isReady()) {
               throw new IllegalStateException("Winner is not ready.");
            }
            result[i] = winner.getCost();
         } else if (this.getOptimizer().getSettings().isColumnUniqueCardinalityPruning()) {
            result[i] = inputGroup.getLowerBound();
         } else {
            result[i] = Cost.zero();
         }
      }
      return result;
   }

   /**
    * Computes and returns a new upper cost bound for the given input based on the old upper cost bound.
    *
    * @param oldUpperBound
    *           old cost bound
    * @param input
    *           input number
    * @return new cost bound
    */
   public Cost getNewUpperBound(final Cost oldUpperBound, final int input) {
      // TODO Implement this method.
      throw new OptimizerError("Method not implemented: OptimizeInputsTask#getNewUpperBound(Cost, int).");
   }

   /**
    * Shortcut method to return the arity of the expression being optimized.
    *
    * @return arity of the expression to optimize
    */
   private int getArity() {
      return this.expression.getOperator().getArity();
   }
}

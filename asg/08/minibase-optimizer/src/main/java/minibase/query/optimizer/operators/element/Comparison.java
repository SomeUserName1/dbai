/*
 * @(#)Comparison.java   1.0   Jun 7, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.element;

import java.util.ArrayList;
import java.util.List;

import minibase.catalog.ColumnStatistics;
import minibase.query.optimizer.LogicalColumnProperties;
import minibase.query.optimizer.LogicalConstantProperties;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.OptimizerError;
import minibase.query.optimizer.StatisticalModel;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.schema.ColumnReference;

/**
 * The {@code Comparison} element operator applies a Boolean operator to its input(s) and returns the result
 * as either {@code true} or {@code false}.
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
 * @author Marcel Hanser &lt;marcel.hanser@uni-konstanz.de&gt;
 */
public class Comparison extends AbstractElementOperator implements BooleanOperator {

   /** Boolean comparison operator used by this comparison element operator. */
   private final ComparisonOperator comparisonOperator;

   /**
    * Creates a new {@code Comparison} element operator that applies the given Boolean comparison operator to
    * its input(s).
    *
    * @param comparisonOperator
    *           Boolean comparison operator
    */
   public Comparison(final ComparisonOperator comparisonOperator) {
      super(OperatorType.COMPARE);
      this.comparisonOperator = comparisonOperator;
   }

   /**
    * Returns the Boolean comparison used in this operator.
    *
    * @return Boolean comparison operator
    */
   public ComparisonOperator getComparisonOperator() {
      return this.comparisonOperator;
   }

   @Override
   public int getArity() {
      if (ComparisonOperator.NOT.equals(this.comparisonOperator)) {
         return 1;
      }
      return 2;
   }

   @Override
   public LogicalProperties getLogicalProperties(final LogicalProperties... inputProperties) {

      // Generate a new set of input columns by adding all input columns from the left and, if it exists,
      // the right input.
      final List<ColumnReference> inputColumns = new ArrayList<>();

      // Get the input properties of the left input.
      final LogicalElementProperties leftProperties = (LogicalElementProperties) inputProperties[0];
      inputColumns.addAll(leftProperties.getInputColumns());
      // Initialize the input properties of the right input to default values.
      LogicalElementProperties rightProperties = LogicalElementProperties.defaultProperties();
      // If there is an actual right input, i.e., if the operator is binary, get the input properties.
      if (this.getArity() > 1) {
         rightProperties = (LogicalElementProperties) inputProperties[1];
         inputColumns.addAll(rightProperties.getInputColumns());
      }

      double selectivity = 0;

      // A unique cardinality of -1 on either side indicates that the cardinality is unknown. If the
      // cardinality were 1 on both sides, this is a comparison of two constants.
      if (!(leftProperties.isConstant() ^ rightProperties.isConstant())
            || ComparisonOperator.LIKE.equals(this.comparisonOperator)) {
         // return selectivity based on defined default values
         selectivity = this.defaultSelectivity(leftProperties, rightProperties);
      } else {
         final LogicalConstantProperties constProps;
         final LogicalColumnProperties colProps;
         if (leftProperties.isConstant()) {
            // Constant value on the left side
            constProps = (LogicalConstantProperties) leftProperties;
            colProps = (LogicalColumnProperties) rightProperties;
         } else {
            // Constant value on the right side
            constProps = (LogicalConstantProperties) rightProperties;
            colProps = (LogicalColumnProperties) leftProperties;
         }
         if (isComparisonOfNumbers(constProps)) {
            // return selectivity based on minimum and maximum values of the column
            selectivity = this.numericSelectivity(constProps, colProps);
         } else {
            // return selectivity based on defined default values
            selectivity = this.defaultSelectivity(constProps, colProps);
         }
      }
      // Logical and, or, and not are handled based on the independence assumption.
      switch (this.comparisonOperator) {
         case AND:
            selectivity = leftProperties.getSelectivity() * rightProperties.getSelectivity();
            break;
         case OR:
            selectivity = leftProperties.getSelectivity() + rightProperties.getSelectivity()
                  - leftProperties.getSelectivity() * rightProperties.getSelectivity();
            break;
         case NOT:
            selectivity = 1 - leftProperties.getSelectivity();
         default:
            // Other operators have been handled above.
      }
      if (selectivity < 0 || selectivity > 1) {
         throw new OptimizerError(String.format("Illegal selectivity value: %.5f.", selectivity));
      }
      // As the result of a comparison is a Boolean value, which cannot be represented, the data type of these
      // logical element properties is null.
      final ColumnStatistics statistics = ColumnStatistics.emptyStatistics();
      return new LogicalElementProperties(statistics, null, 0, selectivity, inputColumns);
   }

   /**
    * @param constProps
    *           constant properties
    * @return {@code true} if the constant value is a {@link Number}
    */
   private static boolean isComparisonOfNumbers(final LogicalConstantProperties constProps) {
      return constProps.getValue() instanceof Number;
   }

   /**
    * Estimates the selectivity based on the minimum, maximum, and the number of distinct values.
    *
    * @param constantProperties
    *           constant properties
    * @param columnProperties
    *           column properties
    * @return the selectivity computed with the help of numeric statistics, e.g. the maximum, minimum and the
    *         number of distinct values
    */
   private double numericSelectivity(final LogicalConstantProperties constantProperties,
         final LogicalColumnProperties columnProperties) {

      final double constant = ((Number) constantProperties.getValue()).doubleValue();
      final Number minimumNumber = (Number) columnProperties.getMinimum();
      final Number maximumNumber = (Number) columnProperties.getMaximum();
      final double distinctValues = columnProperties.getUniqueCardinality();

      if (distinctValues < 0 || Double.isNaN(distinctValues) || minimumNumber == null
            || maximumNumber == null) {
         return this.defaultSelectivity(constantProperties, columnProperties);
      }

      final double minimum = minimumNumber.doubleValue();
      final double maximum = maximumNumber.doubleValue();

      double selectivity = 0;
      switch (this.comparisonOperator) {
         case IN:
            // In this case the right or the left input is a set constant operator and the constant value
            // is the cardinality of that set.
            selectivity = constant / distinctValues;
            break;
         case EQ:
            selectivity = 1 / distinctValues;
            break;
         case NEQ:
            selectivity = 1 - 1 / distinctValues;
            break;
         case LEQ:
         case LT:
            // Selectivity of a "smaller than" range query
            selectivity = (Math.min(maximum, constant) - Math.min(minimum, constant)) / (maximum - minimum);
            break;
         case GEQ:
         case GT:
            // Selectivity of a "larger than" range query
            selectivity = (Math.max(maximum, constant) - Math.max(minimum, constant)) / (maximum - minimum);
            break;
         default:
            // Remaining operators are handled by the next switch statement.
      }
      return selectivity;
   }

   /**
    * Estimates the selectivity based on default values.
    *
    * @param leftProperties
    *           left element properties
    * @param rightProperties
    *           right element properties
    * @return a fall-back selectivity based on no statistics
    */
   private double defaultSelectivity(final LogicalElementProperties leftProperties,
         final LogicalElementProperties rightProperties) {
      final double selectivity;
      switch (this.comparisonOperator) {
         case LIKE:
            if (leftProperties.getUniqueCardinality() == 1) {
               // Left input is a constant.
               final double uniqueCardinality = rightProperties.getUniqueCardinality();
               selectivity = Math.max(StatisticalModel.MINIMUM_SELECTIVITY.getSelectivity(),
                     1 / uniqueCardinality);
            } else if (rightProperties.getUniqueCardinality() == 1) {
               // Right input is a constant.
               final double uniqueCardinality = leftProperties.getUniqueCardinality();
               selectivity = Math.max(StatisticalModel.MINIMUM_SELECTIVITY.getSelectivity(),
                     1 / uniqueCardinality);
            } else {
               // Neither left nor right input is a constant.
               selectivity = StatisticalModel.MINIMUM_SELECTIVITY.getSelectivity();
            }
            break;
         case EQ:
            selectivity = StatisticalModel.EQUALITY_SELECTIVITY.getSelectivity();
            break;
         case NEQ:
            selectivity = 1 - StatisticalModel.EQUALITY_SELECTIVITY.getSelectivity();
            break;
         case LEQ:
         case LT:
         case GEQ:
         case GT:
            selectivity = StatisticalModel.INEQUALITY_SELECTIVITY.getSelectivity();
            break;
         default:
            selectivity = StatisticalModel.INEQUALITY_SELECTIVITY.getSelectivity();
            break;
      }
      return selectivity;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      result.append(this.comparisonOperator.name());
      result.append("]");
      return result.toString();
   }

   /** Enumeration of supported Boolean comparison operators. */
   public enum ComparisonOperator {
      /** Boolean operator <em>AND</em>. */
      AND,
      /** Boolean operator <em>OR</em>. */
      OR,
      /** Boolean operator <em>NOT</em>. */
      NOT,
      /** Boolean operator <em>EQ</em>. */
      EQ,
      /** Boolean operator <em>LT</em>. */
      LT,
      /** Boolean operator <em>LE</em>. */
      LEQ,
      /** Boolean operator <em>GT</em>. */
      GT,
      /** Boolean operator <em>GE</em>. */
      GEQ,
      /** Boolean operator <em>NEQ</em>. */
      NEQ,
      /** Boolean operator <em>LIKE</em>. */
      LIKE,
      /** Boolean operator <em>IN</em>. */
      IN;
   }
}

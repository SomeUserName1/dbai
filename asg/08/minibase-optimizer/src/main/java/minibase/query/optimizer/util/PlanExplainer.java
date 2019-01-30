/*
 * @(#)PlanExplainer.java   1.0   May 24, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.util;

import java.util.Locale;

import minibase.query.optimizer.ExplainedExpression;
import minibase.query.optimizer.Group;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.MultiExpression;
import minibase.query.optimizer.Winner;
import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.rules.RuleType;

/**
 * Utility class to explain a query plan by printing it to an output stream.
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
public final class PlanExplainer {

   /**
    * Formats information about the given group and appends it to the given string builder.
    *
    * @param builder
    *           string builder
    * @param prefix
    *           indentation prefix
    * @param group
    *           expression group
    */
   public static void appendGroup(final StringBuilder builder, final String prefix, final Group group) {
      builder.append(prefix);
      builder.append("Group[id=");
      builder.append(group.getID());
      builder.append(", optimized=");
      builder.append(group.isOptimized());
      builder.append(", explored=");
      builder.append(group.isExplored());
      builder.append(", lowerBound=");
      builder.append(group.getLowerBound());
      builder.append("]\n");
      builder.append(prefix);
      builder.append("Winners\n");
      for (final Winner winner : group.getWinners()) {
         builder.append(prefix + "   " + winner);
         builder.append("\n");
      }
      builder.append(prefix);
      builder.append("Logical Properties\n");
      builder.append("   ");
      final LogicalProperties properties = group.getLogicalProperties();
      if (properties != null) {
         if (properties instanceof LogicalCollectionProperties) {
            final LogicalCollectionProperties collectionProperties = (LogicalCollectionProperties) properties;
            builder.append("cardinality=");
            builder.append(collectionProperties.getCardinality());
            builder.append(", ");
         }
         builder.append("unique=");
         builder.append(properties.getUniqueCardinality());
         builder.append("\n");
      }
      builder.append(prefix);
      builder.append("Logical Expressions\n");
      for (final MultiExpression expression : group.getLogicalExpressions()) {
         appendMultiExpression(builder, prefix + "   ", expression);
         builder.append("\n");
      }
      builder.append("Physical Expressions\n");
      for (final MultiExpression expression : group.getPhysicalExpressions()) {
         appendMultiExpression(builder, prefix + "   ", expression);
         builder.append("\n");
      }
   }

   /**
    * Formats information about the given multi-expression and appends it to the given string builder.
    *
    * @param builder
    *           string builder
    * @param prefix
    *           indentation prefix
    * @param expression
    *           multi-expression
    */
   public static void appendMultiExpression(final StringBuilder builder, final String prefix,
         final MultiExpression expression) {
      builder.append(prefix);
      final Operator operator = expression.getOperator();
      builder.append(operator.toString());
      if (operator.getArity() > 0) {
         builder.append("(");
         for (int i = 0; i < operator.getArity(); i++) {
            final Group group = expression.getInput(i);
            builder.append("[");
            builder.append(group.getID());
            builder.append("]");
            if (i + 1 < operator.getArity()) {
               builder.append(", ");
            }
         }
         builder.append(")   (bitmask=");
         appendBitmask(builder, expression.getBitmask());
         builder.append(")");
      }
   }

   /**
    * Appends the bit-mask represented as an integer as a string of {@code 0} and {@code 1} to the given
    * builder.
    *
    * @param builder
    *           string builder
    * @param bitmask
    *           bit-mask
    */
   private static void appendBitmask(final StringBuilder builder, final int bitmask) {
      for (int i = RuleType.values().length - 1; i >= 0; i--) {
         if ((1 << i & bitmask) > 0) {
            builder.append("1");
         } else {
            builder.append("0");
         }
      }
   }

   /**
    * Formats information about the given query expression and appends it to the given string builder.
    *
    * @param builder
    *           string builder
    * @param prefix
    *           indentation prefix
    * @param expression
    *           query expression
    */
   public static void appendExplainedExpression(final StringBuilder builder, final String prefix,
         final ExplainedExpression expression) {
      builder.append(expression.getOperator());
      builder.append("   (cost=");
      builder.append(expression.getCost());
      builder.append(", i/o=");
      builder.append(String.format(Locale.US, "%.3f", expression.getCost().getIO()));
      builder.append(", cpu=");
      builder.append(String.format(Locale.US, "%.5f", expression.getCost().getCPU()));
      builder.append(", card=");
      builder.append(expression.getCardinality());
      builder.append(", ucard=");
      builder.append(expression.getUniqueCardinality());
      builder.append(", twidth=");
      builder.append(String.format(Locale.US, "%.3f", expression.getWidth()));
      builder.append(")\n");
      for (int i = 0; i < expression.getOperator().getArity(); i++) {
         builder.append(prefix);
         if (i + 1 < expression.getOperator().getArity()) {
            builder.append("\u251C");
         } else {
            builder.append("\u2514");
         }
         builder.append("\u2500 ");
         final String newPrefix = prefix + (i < expression.getOperator().getArity() - 1 ? "\u2502  " : "   ");
         appendExplainedExpression(builder, newPrefix, (ExplainedExpression) expression.getInput(i));
      }
   }

   /**
    * Hidden constructor.
    */
   private PlanExplainer() {
      // hidden constructor
   }
}

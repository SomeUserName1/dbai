/*
 * @(#)ExpressionColumnReference.java   1.0   Jun 12, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.List;
import java.util.Optional;

import minibase.catalog.DataType;
import minibase.catalog.SystemCatalog;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.LogicalElementProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.operators.logical.LogicalOperator;

/**
 * Reference to a query expression.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class ExpressionColumnReference extends AbstractReference implements DerivedColumnReference {

   /** Name of this reference. */
   private final String name;

   /** Referenced expression. */
   private final Expression expression;

   /** Logical element properties. */
   private final LogicalElementProperties properties;

   /** System catalog. */
   private final SystemCatalog catalog;

   /**
    * Creates a new reference with the given id and name to the given expression.
    *
    * @param id
    *           ID of the reference
    * @param name
    *           name of the reference
    * @param expression
    *           referenced expression
    * @param catalog
    *           system catalog
    */
   ExpressionColumnReference(final int id, final String name, final Expression expression,
         final SystemCatalog catalog) {
      super(id);
      this.name = name;
      this.expression = expression;
      this.properties = initLogicalProperties(expression);
      this.catalog = catalog;
   }

   @Override
   public String getName() {
      return this.name;
   }

   /**
    * Returns the query expression that is referenced by this column reference.
    *
    * @return query expression
    */
   public Expression getExpression() {
      return this.expression;
   }

   @Override
   public Optional<TableReference> getParent() {
      return Optional.empty();
   }

   @Override
   public DataType getType() {
      return this.properties.getType();
   }

   @Override
   public int getSize() {
      return this.properties.getSize();
   }

   @Override
   public double getWidth() {
      return (double) this.properties.getSize() / this.catalog.getPageSize();
   }

   @Override
   public List<ColumnReference> getInputColumns() {
      return this.properties.getInputColumns();
   }

   @Override
   public boolean isAggregation() {
      return OperatorType.AGGREGATION_FUNCTION.equals(this.expression.getOperator().getType());
   }

   @Override
   public String toString() {
      return this.name;
   }

   /**
    * This method plays optimizer and initializes the logical properties of the given expression.
    *
    * @param expression
    *           logical expression
    * @return logical properties
    */
   private static LogicalElementProperties initLogicalProperties(final Expression expression) {
      final LogicalOperator operator = (LogicalOperator) expression.getOperator();
      if (!operator.isElement()) {
         throw new IllegalStateException("Operator " + operator.getName() + " is not an element operator.");
      }
      final int arity = operator.getArity();
      if (arity == 0) {
         return (LogicalElementProperties) operator.getLogicalProperties();
      }
      final LogicalProperties[] inputLogicalProperties = new LogicalProperties[arity];
      for (int i = 0; i < arity; i++) {
         inputLogicalProperties[i] = initLogicalProperties(expression.getInput(i));
      }
      return (LogicalElementProperties) operator.getLogicalProperties(inputLogicalProperties);
   }
}

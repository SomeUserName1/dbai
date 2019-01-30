/*
 * @(#)QueryExpression.java   1.0   May 28, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.parser;

import java.util.ArrayList;
import java.util.List;

import minibase.query.QueryException;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.operators.Operator;
import minibase.query.schema.ReferenceTable;

/**
 * Node type representing an expression in the abstract syntax tree.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt
 * @version 1.0
 */
public class QueryExpression extends SimpleNode {

   /**
    * Creates a new expression node.
    *
    * @param id
    *           node identifier
    */
   public QueryExpression(final int id) {
      super(id);
   }

   /**
    * Creates a new expression node.
    *
    * @param p
    *           parser that generated this node
    * @param id
    *           node identifier
    */
   public QueryExpression(final QueryExpressionParser p, final int id) {
      super(p, id);
   }

   /**
    * Returns the operator of this query expression.
    *
    * @return operator node
    */
   public QueryOperator getQueryOperator() {
      return (QueryOperator) this.jjtGetChild(0);
   }

   /**
    * Returns the input expressions of this query expression.
    *
    * @return input expressions
    */
   public List<QueryExpression> getQueryExpression() {
      final List<QueryExpression> inputs = new ArrayList<>();
      for (int i = 1; i < this.jjtGetNumChildren(); i++) {
         inputs.add((QueryExpression) this.jjtGetChild(i));
      }
      return inputs;
   }

   /**
    * Appends a string representation of this query expression to the given string builder.
    *
    * @param result
    *           string builder
    * @param prefix
    *           indentation prefix
    */
   protected void toString(final StringBuilder result, final String prefix) {
      if (this.getQueryExpression().isEmpty()) {
         // Leaf node
         result.append(prefix);
         this.getQueryOperator().toString(result);
      } else {
         // Inner node
         result.append(prefix + "(");
         this.getQueryOperator().toString(result);
         result.append(",\n");
         for (int i = 0; i < this.getQueryExpression().size(); i++) {
            final QueryExpression expression = this.getQueryExpression().get(i);
            expression.toString(result, prefix + "   ");
            if (i + 1 < this.getQueryExpression().size()) {
               result.append(",");
            }
            result.append("\n");
         }
         result.append(prefix + ")");
      }
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder();
      this.toString(result, "");
      return result.toString();
   }

   /**
    * Converts this query expression into an optimizer expression. The given query catalog is used to resolves
    * symbols in the abstract syntax tree.
    *
    * @param catalog
    *           query catalog
    * @return optimizer expression
    * @throws QueryException exception
    */
   public Expression toExpression(final ReferenceTable catalog) throws QueryException {
      final List<QueryExpression> queryExpressions = this.getQueryExpression();
      final Expression[] inputs = new Expression[queryExpressions.size()];
      for (int i = 0; i < inputs.length; i++) {
         inputs[i] = queryExpressions.get(i).toExpression(catalog);
      }
      final Operator operator = this.getQueryOperator().toOperator(catalog);
      if (operator == null && inputs.length == 1) {
         return inputs[0];
      }
      return new Expression(operator, inputs);
   }
}

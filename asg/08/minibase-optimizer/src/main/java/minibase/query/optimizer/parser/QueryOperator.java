/*
 * @(#)QueryOperator.java   1.0   May 28, 2016
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
import java.util.Optional;

import minibase.catalog.DataType;
import minibase.query.AggregationType;
import minibase.query.QueryException;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.operators.element.AggregationFunction;
import minibase.query.optimizer.operators.element.Comparison;
import minibase.query.optimizer.operators.element.Comparison.ComparisonOperator;
import minibase.query.optimizer.operators.element.Constant;
import minibase.query.optimizer.operators.element.GetColumn;
import minibase.query.optimizer.operators.logical.Aggregation;
import minibase.query.optimizer.operators.logical.Distinct;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.query.optimizer.operators.logical.Projection;
import minibase.query.optimizer.operators.logical.Selection;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.DerivedColumnReference;
import minibase.query.schema.ReferenceTable;
import minibase.query.schema.StoredColumnReference;
import minibase.query.schema.StoredTableReference;

/**
 * Node type representing an expression operator in the abstract syntax tree.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class QueryOperator extends SimpleNode {

   /** Type of this operator. */
   private OperatorType type;

   /** Arguments of this operator. */
   private Object[] args;

   /**
    * Creates a new expression operator node.
    *
    * @param id
    *           node identifier
    */
   public QueryOperator(final int id) {
      super(id);
   }

   /**
    * Creates a new expression operator node.
    *
    * @param p
    *           parser that generated this node
    * @param id
    *           node identifier
    */
   public QueryOperator(final QueryExpressionParser p, final int id) {
      super(p, id);
   }

   /**
    * Sets the type of this operator.
    *
    * @param type
    *           operator type.
    */
   protected void setType(final OperatorType type) {
      this.type = type;
   }

   /**
    * Returns the type of this operator.
    *
    * @return operator type.
    */
   public OperatorType getType() {
      return this.type;
   }

   /**
    * Sets the arguments of this operator.
    *
    * @param args
    *           operator arguments
    */
   protected void setArguments(final Object... args) {
      this.args = args;
   }

   /**
    * Returns the arguments of this operator.
    *
    * @return operator arguments
    */
   public Object[] getArguments() {
      return this.args;
   }

   /**
    * Appends a string representation of this query operator to the given string builder.
    *
    * @param result
    *           string builder
    */
   protected void toString(final StringBuilder result) {
      switch (this.type) {
         case AGGREGATE:
            result.append("AGG_LIST((");
            this.appendArgumentList(result, (List< ? >) this.args[0]);
            result.append("), GROUP_BY(");
            this.appendArgumentList(result, (List< ? >) this.args[1]);
            result.append("))");
            return;
         case COMPARE:
            result.append("OP_" + this.args[0]);
            return;
         case CONSTANT:
            String value = (String) this.args[0];
            final DataType type = (DataType) this.args[1];
            if (DataType.CHAR.equals(type)) {
               result.append("STR");
               value = "\"" + value + "\"";
            } else {
               result.append(type.name());
            }
            result.append("(");
            result.append(value);
            result.append(")");
            return;
         case GETCOLUMN:
            result.append("ATTR");
            break;
         case GETTABLE:
            final Optional< ? > alias = (Optional< ? >) this.args[1];
            result.append("GET(");
            result.append(this.args[0]);
            if (alias.isPresent()) {
               result.append(", ");
               result.append(alias.get());
            }
            result.append(")");
            return;
         case SORT:
            result.append("ORDER_BY");
            break;
         default:
            result.append(this.type);
      }
      if (this.args != null && this.args.length > 0) {
         result.append("(");
         for (int i = 0; i < this.args.length; i++) {
            final Object arg = this.args[i];
            if (arg instanceof List< ? >) {
               this.appendArgumentList(result, (List< ? >) arg);
            } else {
               result.append(arg.toString());
            }
            if (i + 1 < this.args.length) {
               result.append(", ");
            }
         }
         result.append(")");
      }
   }

   /**
    * Appends the given argument list to the given string builder.
    *
    * @param result
    *           string builder
    * @param args
    *           argument list
    */
   private void appendArgumentList(final StringBuilder result, final List< ? > args) {
      if (args.size() == 0 || args.size() > 1) {
         result.append("<");
         for (int j = 0; j < args.size(); j++) {
            result.append(args.get(j));
            if (j + 1 < args.size()) {
               result.append(", ");
            }
         }
         result.append(">");
      } else {
         result.append(args.get(0));
      }
   }

   /**
    * Converts this query operator into an optimizer operator. The given query catalog is used to resolves
    * symbols in the abstract syntax tree.
    *
    * @param catalog
    *           query catalog
    * @return optimizer operator
    * @throws QueryException exception
    */
   public Operator toOperator(final ReferenceTable catalog) throws QueryException {
      switch (this.type) {
         case GETTABLE:
            return this.toGetTable(catalog);
         case GETCOLUMN:
            return this.toGetColumn(catalog);
         case CONSTANT:
            return this.toConstant();
         case COMPARE:
            return this.toComparison();
         case SELECT:
            return this.toSelection();
         case EQJOIN:
            return this.toEquiJoin(catalog);
         case AGGREGATE:
            return this.toAggregation(catalog);
         case PROJECT:
            return this.toProjection(catalog);
         case DISTINCT:
            return new Distinct();
         default:
            throw new IllegalStateException("Unsupported operator type: " + this.type);
      }
   }

   /**
    * Converts this query operator into a get-table optimizer operator. The given query catalog is used to
    * resolves symbols in the abstract syntax tree.
    *
    * @param catalog
    *           query catalog
    * @return get-table operator
    * @throws QueryException exception
    */
   private GetTable toGetTable(final ReferenceTable catalog) throws QueryException {
      final String name = (String) this.args[0];
      final Optional< ? > alias = (Optional< ? >) this.args[1];
      final StoredTableReference table;
      if (alias.isPresent()) {
         table = catalog.insertTable(name, (String) alias.get());
      } else {
         table = catalog.insertTable(name);
      }
      return new GetTable(table);
   }

   /**
    * Converts this query operator to a get-column optimizer operator.
    *
    * @param references
    *           reference table
    * @return get-column operator
    */
   private GetColumn toGetColumn(final ReferenceTable references) {
      final QueryColumn column = (QueryColumn) this.args[0];
      return new GetColumn((StoredColumnReference) resolve(references, column).get());
   }

   /**
    * Converts this query operator to a constant optimizer operator.
    *
    * @return constant operator
    */
   private Constant toConstant() {
      final String value = (String) this.args[0];
      final DataType type = (DataType) this.args[1];
      int size = type.getSize();
      if (DataType.VARCHAR.equals(type) || DataType.CHAR.equals(type)) {
         size = value.length();
      }
      return new Constant(type.parse(value), type, size);
   }

   /**
    * Converts this query operator to a comparison optimizer operator.
    *
    * @return comparison operator
    */
   private Comparison toComparison() {
      final ComparisonOperator op = (ComparisonOperator) this.args[0];
      return new Comparison(op);
   }

   /**
    * Converts this query operator to a selection optimizer operator.
    *
    * @return selection operator
    */
   private Selection toSelection() {
      return new Selection();
   }

   /**
    * Converts this query operator to an equi-join optimizer operator.
    *
    * @param references
    *           reference table
    * @return equi-join operator
    */
   @SuppressWarnings("unchecked")
   private EquiJoin toEquiJoin(final ReferenceTable references) {
      final List<QueryColumn> leftColumns = (List<QueryColumn>) this.args[0];
      final List<QueryColumn> rightColumns = (List<QueryColumn>) this.args[1];
      return new EquiJoin(resolve(references, leftColumns), resolve(references, rightColumns));
   }

   /**
    * Converts this query operator to an aggregation optimizer operator.
    *
    * @param references
    *           reference table
    * @return aggregation operator
    */
   @SuppressWarnings("unchecked")
   private Aggregation toAggregation(final ReferenceTable references) {
      final List<QueryDerivedColumn> derivedColumns = (List<QueryDerivedColumn>) this.args[0];
      final List<QueryColumn> groupByColumns = (List<QueryColumn>) this.args[1];
      final List<DerivedColumnReference> aggregatedColumns = new ArrayList<>();
      for (final QueryDerivedColumn derivedColumn : derivedColumns) {
         final List<ColumnReference> inputColumns = resolve(references, derivedColumn.getInputs());
         final List<Expression> inputs = new ArrayList<>();
         for (final ColumnReference inputColumn : inputColumns) {
            inputs.add(new Expression(new GetColumn((StoredColumnReference) inputColumn)));
         }
         // TODO Remove hard-coding to SUM aggregation function
         final Expression expression = new Expression(
               new AggregationFunction(AggregationType.SUM, inputs.size()), inputs);
         aggregatedColumns.add(references.insertColumn(derivedColumn.getName(), expression));
      }
      return new Aggregation(resolve(references, groupByColumns), aggregatedColumns);
   }

   /**
    * Converts this query operator to a projection optimizer operator.
    *
    * @param references
    *           reference table
    * @return projection operator
    */
   @SuppressWarnings("unchecked")
   private Projection toProjection(final ReferenceTable references) {
      final List<QueryColumn> projectionColumns = (List<QueryColumn>) this.args[0];
      return new Projection(resolve(references, projectionColumns));
   }

   /**
    * Resolves the given query column based on the given reference table.
    *
    * @param references
    *           reference table
    * @param column
    *           query column
    * @return resolved column
    */
   private static Optional<ColumnReference> resolve(final ReferenceTable references, final QueryColumn column) {
      if (column.getScope().isPresent()) {
         return references.resolveColumn(column.getScope().get(), column.getName());
      }
      return references.resolveColumn(column.getName());
   }

   /**
    * Resolves the given list of query columns based on the given reference table.
    *
    * @param references
    *           reference table
    * @param columns
    *           query column list
    * @return list of resolved column
    */
   private static List<ColumnReference> resolve(final ReferenceTable references,
         final List<QueryColumn> columns) {
      final List<ColumnReference> result = new ArrayList<>();
      for (final QueryColumn column : columns) {
         result.add(resolve(references, column).get());
      }
      return result;
   }
}

/*
 * @(#)QueryDerivedColumn.java   1.0   May 28, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.parser;

import java.util.List;

/**
 * Utility class to represent derived columns in aggregate operators during parsing.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class QueryDerivedColumn {

   /** Name of the derived column. */
   private final String name;

   /** Input columns of the derived column. */
   private final List<QueryColumn> inputs;

   /**
    * Creates a new derived column with the given name that is derived from the given inputs.
    *
    * @param name
    *           column name
    * @param inputs
    *           input columns
    */
   protected QueryDerivedColumn(final String name, final List<QueryColumn> inputs) {
      this.name = name;
      this.inputs = inputs;
   }

   /**
    * Returns the name of this derived column.
    *
    * @return column name
    */
   public String getName() {
      return this.name;
   }

   /**
    * Returns the input columns of this derived column.
    *
    * @return input columns
    */
   public List<QueryColumn> getInputs() {
      return this.inputs;
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder();
      if (this.inputs.size() == 0 || this.inputs.size() > 1) {
         result.append("<");
         for (int i = 0; i < this.inputs.size(); i++) {
            result.append(this.inputs.get(i));
            if (i + 1 < this.inputs.size()) {
               result.append(", ");
            }
         }
         result.append(">");
      } else {
         result.append(this.inputs.get(0));
      }
      result.append(" AS ");
      result.append(this.name);
      return result.toString();
   }
}

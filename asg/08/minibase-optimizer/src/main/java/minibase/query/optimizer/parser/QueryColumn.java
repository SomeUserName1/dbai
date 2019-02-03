/*
 * @(#)QueryColumn.java   1.0   May 28, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.parser;

import java.util.Optional;

/**
 * Utility class to represent columns during parsing.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class QueryColumn {

   /** Identifier name. */
   private final String name;

   /** Identifier scope. */
   private final Optional<String> scope;

   /**
    * Creates a new column with the given scope and name.
    *
    * @param scope
    *           column scope, or {@code null} for derived columns
    * @param name
    *           column name
    */
   protected QueryColumn(final String scope, final String name) {
      this.name = name;
      this.scope = Optional.ofNullable(scope);
   }

   /**
    * Returns the name of the column.
    *
    * @return column name
    */
   public String getName() {
      return this.name;
   }

   /**
    * Returns the scope of the column.
    *
    * @return column scope
    */
   public Optional<String> getScope() {
      return this.scope;
   }

   @Override
   public String toString() {
      final StringBuilder result = new StringBuilder();
      if (this.scope.isPresent()) {
         result.append(this.scope.get());
      }
      result.append(".");
      result.append(this.name);
      return result.toString();
   }
}

/*
 * @(#)AbstractOperator.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import minibase.query.schema.Schema;

/**
 * Query execution is driven by a tree of relational operators, all of which are implemented as iterators.
 * Results are requested by successive "get next tuple" calls on the root of the tree, which in turn makes
 * similar calls to child iterators throughout the tree. Intermediate nodes (i.e. join iterators) drive the
 * leaf-level nodes (i.e. file or index scan iterators).
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 * @since 1.0
 */
public abstract class AbstractOperator implements Operator {

   /** Schema for resulting tuples; must be set in all subclass constructors. */
   private final Schema schema;

   /**
    * Constructor setting the schema.
    *
    * @param schema
    *           the schema of the tuples returned by this iterator
    */
   AbstractOperator(final Schema schema) {
      this.schema = schema;
   }

   /**
    * Schema of the tuples returned by this iterator.
    *
    * @return the schema
    */
   @Override
   public Schema getSchema() {
      return this.schema;
   }
}

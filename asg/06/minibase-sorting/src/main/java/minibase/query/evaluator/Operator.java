/*
 * @(#)Operator.java   1.0   Jun 04, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import minibase.query.schema.Schema;

/**
 * Implementation of an operator in the query evaluator.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public interface Operator {

   /**
    * Opens a new iterator that evaluates this operator.
    *
    * @return open iterator
    */
   TupleIterator open();

   /**
    * Returns the schema of the tuples returned by this iterator.
    *
    * @return schema of the returned tuples
    */
   Schema getSchema();
}

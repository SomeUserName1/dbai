/*
 * @(#)Predicate.java   1.0   Jun 17, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator.predicate;

import java.util.Optional;

import minibase.catalog.DataType;
import minibase.query.QueryException;
import minibase.query.schema.Schema;

/**
 * Abstract predicate for recursive evaluation of predicates.
 *
 * @param <T>
 *           type of operand
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 * @author Michael Delz &lt;michael.delz@uni.kn&gt;
 * @version 1.0
 */
public abstract class Predicate<T> {

   /**
    * Evaluate the predicate recursively.
    *
    * @param tuple
    *           tuple to evaluate on
    * @return result of predicate, together with its data type
    */
   public abstract Operand<T> evaluate(byte[] tuple);

   /**
    * Evaluate the predicate recursively.
    *
    * @param tuple
    *           tuple to evaluate on
    * @return result of predicate
    */
   public abstract T eval(byte[] tuple);

   /**
    * Validate predicate.
    *
    * @param schema
    *           schema to validate predicate against
    * @return Data type of predicate
    * @throws QueryException
    *            when predicate is invalid
    */
   public abstract Optional<DataType> validate(Schema schema) throws QueryException;

   @Override
   public abstract String toString();
}

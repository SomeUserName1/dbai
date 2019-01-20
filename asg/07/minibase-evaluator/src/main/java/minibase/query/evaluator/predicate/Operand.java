/*
 * @(#)Operand.java   1.0   Mar 22, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator.predicate;

import minibase.catalog.DataType;

/**
 * This is the Operand handled by the predicate, it encapsulates a value and its type.
 *
 * @param <T> type of operand
 *
 * @author Johann Bornholdt &lt;johann.bornholdt@uni-konstanz.de&gt;
 * @version 1.0
 */
public class Operand<T> {

   /** Value. */
   private final T value;

   /** Type. */
   private final DataType type;

   /**
    * Constructor.
    * 
    * @param value
    *           value
    * @param type
    *           type
    */
   public Operand(final T value, final DataType type) {
      this.value = value;
      this.type = type;
   }

   /**
    * Return value.
    * @return value
    */
   public T getValue() {
      return this.value;
   }

   /**
    * Return type.
    * @return type
    */
   public DataType getType() {
      return this.type;
   }
}

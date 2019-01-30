/*
 * @(#)JoinMode.java   1.0   Mar 3, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

/**
 * Enumeration describing the different joins modes of an equi-join.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public enum JoinMode {

   /** General, i.e., no foreign key relationship exists. */
   GENERAL(2),
   /** Source foreign key, i.e., the columns are only in the source table. */
   SOURCE_FK(1),
   /** Full foreign key, i.e., the join columns are in the foreign and referenced key. */
   FULL_FK(0);

   /** Priority of this join mode, smaller is higher. */
   private final int priority;

   /**
    * Constructs a new join mode with the given priority.
    *
    * @param priority
    *           priority of this join mode
    */
   JoinMode(final int priority) {
      this.priority = priority;
   }

   /**
    * Returns the priority of this join mode, smaller is higher.
    *
    * @return priority of this join mode
    */
   public int getPriority() {
      return this.priority;
   }
}

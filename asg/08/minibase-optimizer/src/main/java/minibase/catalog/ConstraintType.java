/*
 * @(#)ConstraintType.java   1.0   Aug 27, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

/**
 * Enumeration that defines the constraint types supported by the Minibase catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public enum ConstraintType {

   /** Not null constraint. */
   NOT_NULL,
   /** Primary key constraint. */
   UNIQUE,
   /** Foreign key constraint. */
   PRIMARY_KEY,
   /** Unique constraint. */
   FOREIGN_KEY,
   /** Check constraint. */
   CHECK,
   /** Default constraint. */
   DEFAULT;
}

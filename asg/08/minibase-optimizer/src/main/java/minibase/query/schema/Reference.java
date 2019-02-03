/*
 * @(#)Reference.java   1.0   Feb 26, 2015
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
 * References are used during query processing to assign names to system catalog object and subexpressions.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public interface Reference {

   /**
    * Returns the ID of this symbol.
    *
    * @return symbol ID
    */
   int getID();

   /**
    * Returns the name given to this reference.
    *
    * @return reference name
    */
   String getName();

   /**
    * Returns the declared size of this reference in bytes.
    *
    * @return declared size in bytes
    */
   int getSize();

   /**
    * Returns the declared width of this reference as a fraction of a block. This is a property of the schema
    * and not to be confused with the <em>average width</em>, which is a statistical value and accessed via
    * {@code TableStatistics} or {@code ColumnStatistics}.
    *
    * @return declared width (as a fraction of a block)
    */
   double getWidth();
}

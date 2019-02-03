/*
 * @(#)Descriptor.java   1.0   Aug 30, 2014
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
 * Common interface of all descriptors of the Minibase system catalog. Every descriptor has a reference to the
 * system catalog that created it as well as an immutable identifier.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public interface Descriptor {

   /**
    * Returns a reference to the system catalog that manages this descriptor.
    *
    * @return system catalog of this descriptor
    */
   SystemCatalog getSystemCatalog();

   /**
    * Returns the globally unique ID of the catalog element described by this descriptor.
    *
    * @return globally unique ID of this descriptor
    */
   int getCatalogID();

   /**
    * Returns the SQL data definition statement that recreates this system catalog descriptor.
    *
    * @return SQL data definition statement
    */
   String toSQL();
}

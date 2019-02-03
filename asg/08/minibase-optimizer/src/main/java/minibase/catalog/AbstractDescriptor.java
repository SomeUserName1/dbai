/*
 * @(#)AbstractDescriptor.java   1.0   Aug 30, 2014
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
 * Common abstract superclass of all descriptors of the Minibase system catalog. Every descriptor has a
 * reference to the system catalog that created it as well as an immutable identifier.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public abstract class AbstractDescriptor implements Descriptor {

   /** Reference to the system catalog that manages this descriptor. */
   private final SystemCatalog catalog;

   /** Globally unique ID of the catalog element described by this descriptor. */
   private final int catalogID;

   /**
    * Constructs a new descriptor for a system catalog object with the given identifier.
    *
    * @param catalog
    *           system catalog of this descriptor
    * @param catalogID
    *           unique ID of this descriptor
    */
   AbstractDescriptor(final SystemCatalog catalog, final int catalogID) {
      this.catalog = catalog;
      this.catalogID = catalogID;
   }

   /**
    * Returns a reference to the system catalog that manages this descriptor.
    *
    * @return system catalog of this descriptor
    */
   @Override
   public SystemCatalog getSystemCatalog() {
      return this.catalog;
   }

   /**
    * Returns the globally unique ID of the catalog element described by this descriptor.
    *
    * @return globally unique ID of this descriptor
    */
   @Override
   public int getCatalogID() {
      return this.catalogID;
   }
}

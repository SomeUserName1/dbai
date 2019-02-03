/*
 * @(#)NamedDescriptor.java   1.0   Jan 3, 2014
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
 * Common abstract superclass of all descriptors of the Minibase system catalog that have a name.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public abstract class NamedDescriptor extends AbstractDescriptor {

   /** Qualified name of the catalog element described by this descriptor. */
   private String name;

   /** Field to indicate whether the name of the descriptor is internal. */
   private final boolean internalName;

   /**
    * Constructs a new descriptor for a catalog element with the given name and the given globally unique ID.
    *
    * @param catalog
    *           system catalog of this descriptor
    * @param id
    *           unique ID of this descriptor
    * @param name
    *           name of this descriptor
    */
   NamedDescriptor(final SystemCatalog catalog, final int id, final String name) {
      super(catalog, id);
      if (name != null && name.length() > 0) {
         this.name = name;
         this.internalName = false;
      } else {
         this.name = this.getInternalName(id);
         this.internalName = true;
      }
   }

   /**
    * Returns an internal name for this descriptor based on the given ID.
    *
    * @param id
    *           ID of the descriptor
    * @return internal name of the descriptor
    */
   private String getInternalName(final int id) {
      final String prefix = this.getClass().getSimpleName();
      return prefix.substring(0, prefix.length() - 10) + id;
   }

   /**
    * Sets the name of this descriptor to the given string.
    *
    * @param name
    *           new descriptor name
    */
   void setName(final String name) {
      this.name = name;
   }

   /**
    * Returns the name of the catalog element described by this descriptor.
    *
    * @return name of this descriptor
    */
   public String getName() {
      return this.name;
   }

   /**
    * Returns whether the name of this descriptor is an internal name.
    *
    * @return {@code true} if the descriptor's name is internal, {@code false} otherwise.
    */
   boolean isNameInternal() {
      return this.internalName;
   }
}

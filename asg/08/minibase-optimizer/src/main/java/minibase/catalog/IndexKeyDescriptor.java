/*
 * @(#)IndexKeyDescriptor.java   1.0   Aug 29, 2014
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
 * Descriptor for an index key in the Minibase system catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public final class IndexKeyDescriptor extends AbstractDescriptor implements OwnedDescriptor {

   /** Sort key. */
   private final CatalogSortKey key;

   /** Identifier of the owner object. */
   private final int ownerID;

   /**
    * Constructs a new primary key descriptor with the given globally unique ID and the identified attribute
    * as key.
    *
    * @param catalog
    *           system catalog of this primary key
    * @param id
    *           globally unique ID of this primary key
    * @param ownerID
    *           ID of the owner object
    * @param keyColumnIDs
    *           IDs of the key columns
    * @param orders
    *           order of the key columns, if applicable
    */
   public IndexKeyDescriptor(final SystemCatalog catalog, final int id, final int ownerID,
         final int[] keyColumnIDs, final SortOrder[] orders) {
      super(catalog, id);
      this.ownerID = ownerID;
      this.key = new CatalogSortKey(keyColumnIDs, orders);
   }

   /**
    * Returns the sort key defined by this index key.
    *
    * @return sort key
    */
   public CatalogSortKey getKey() {
      return this.key;
   }

   @Override
   public int getOwnerID() {
      return this.ownerID;
   }

   @Override
   public String toSQL() {
      // TODO implement IndexKeyDescriptor.toSQL
      return this.toString();
   }
}

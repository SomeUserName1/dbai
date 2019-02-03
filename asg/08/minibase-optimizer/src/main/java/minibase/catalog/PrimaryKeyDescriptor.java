/*
 * @(#)PrimaryKeyDescriptor.java   1.0   Jan 4, 2014
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
 * Descriptor for a primary key in the Minibase system catalog.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class PrimaryKeyDescriptor extends ConstraintDescriptor {

   /** Primary key. */
   private final CatalogKey key;

   /**
    * Constructs a new primary key descriptor as a table constraint with the given identifier, name, owner
    * table identifier and key column identifiers.
    *
    * @param catalog
    *           system catalog of this primary key
    * @param id
    *           identifier of this primary key
    * @param name
    *           name of this primary key
    * @param ownerID
    *           identifier of the owner table
    * @param columnIDs
    *           identifiers of the key columns
    */
   public PrimaryKeyDescriptor(final SystemCatalog catalog, final int id, final String name,
         final int ownerID, final int[] columnIDs) {
      this(catalog, id, name, SystemCatalog.INVALID_ID, ownerID, columnIDs);
   }

   /**
    * Constructs a new primary key descriptor as a column constraint with the given identifier, name, owner
    * table identifier and key column identifiers.
    *
    * @param catalog
    *           system catalog of this primary key
    * @param id
    *           identifier of this primary key
    * @param name
    *           name of this primary key
    * @param ownerID
    *           identifier of the owner table
    * @param columnID
    *           identifier of the key column
    */
   public PrimaryKeyDescriptor(final SystemCatalog catalog, final int id, final String name,
         final int ownerID, final int columnID) {
      this(catalog, id, name, columnID, ownerID, new int[] { columnID });
   }

   /**
    * Constructs a new primary key descriptor with the given identifier, name, constrained column identifier,
    * owner identifier, and key column identifiers.
    *
    * @param catalog
    *           system catalog of this primary key
    * @param id
    *           identifier of this primary key
    * @param name
    *           name of this primary key
    * @param columnID
    *           identifier of the constrained column
    * @param ownerID
    *           identifier of the owner table
    * @param columnIDs
    *           identifiers of the key columns
    */
   private PrimaryKeyDescriptor(final SystemCatalog catalog, final int id, final String name,
         final int columnID, final int ownerID, final int[] columnIDs) {
      super(catalog, id, name, columnID, ownerID);
      if (columnIDs.length < 1) {
         throw new IllegalArgumentException("Primary key constaint must consist of at least one attribute.");
      }
      if (!this.isTableConstraint() && columnIDs.length > 1) {
         throw new IllegalArgumentException(
               "Column primary key constraint must consist of exactly one attribute.");
      }
      this.key = new CatalogKey(columnIDs);
   }

   /**
    * Returns the identifiers of the key attributes.
    *
    * @return identifiers of the key attributes
    */
   public CatalogKey getKey() {
      return this.key;
   }

   @Override
   public String toSQL() {
      final StringBuilder result = new StringBuilder();
      this.formatHeader(result);
      result.append("PRIMARY KEY");
      if (this.isTableConstraint()) {
         result.append(" ");
         this.formatKeyColumnNames(result, this.key);
      }
      return result.toString();
   }
}

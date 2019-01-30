/*
 * @(#)ForeignKeyDescriptor.java   1.0   Jan 3, 2014
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
 * Descriptor for a foreign key in the Minibase system catalog. A foreign key descriptor provides read-only
 * information about the columns of the foreign key in both tables as well as the identifier of the referenced
 * table.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class ForeignKeyDescriptor extends ConstraintDescriptor {

   /** Foreign key. */
   private final CatalogForeignKey key;

   /** Identifier of the referenced table. */
   private final int referencedTableID;

   /**
    * Constructs a new foreign key descriptor as a table constraint with the given identifier, name, owner
    * table identifier, referencing key column identifiers, referenced table identifier, and referenced column
    * identifiers.
    *
    * @param catalog
    *           system catalog of this foreign key
    * @param id
    *           globally unique ID of this foreign key
    * @param name
    *           name of this foreign key
    * @param ownerID
    *           identifier of the owner table
    * @param columnIDs
    *           identifiers of the referencing columns
    * @param referencedTableID
    *           identifier of the table containing the referenced columns
    * @param referencedIDs
    *           identifiers of the referenced columns
    */
   public ForeignKeyDescriptor(final SystemCatalog catalog, final int id, final String name,
         final int ownerID, final int[] columnIDs, final int referencedTableID, final int[] referencedIDs) {
      this(catalog, id, name, SystemCatalog.INVALID_ID, ownerID, columnIDs, referencedTableID, referencedIDs);
   }

   /**
    * Constructs a new foreign key descriptor as a column constraint with the given identifier, name, owner
    * table identifier, referencing key column identifier, referenced table identifier, and referenced column
    * identifier.
    *
    * @param catalog
    *           system catalog of this foreign key
    * @param id
    *           globally unique ID of this foreign key
    * @param name
    *           name of this foreign key
    * @param ownerID
    *           identifier of the owner table
    * @param columnID
    *           identifier of the referencing column
    * @param referencedTableID
    *           identifier of the table containing the referenced column
    * @param referencedID
    *           identifier of the referenced column
    */
   public ForeignKeyDescriptor(final SystemCatalog catalog, final int id, final String name,
         final int ownerID, final int columnID, final int referencedTableID, final int referencedID) {
      this(catalog, id, name, columnID, ownerID, new int[] { columnID }, referencedTableID,
            new int[] { referencedID });
   }

   /**
    * Constructs a new foreign key descriptor with the given identifier, name, constrained column identifier,
    * owner table identifier, referencing key column identifier, referenced table identifier, and referenced
    * column identifier.
    *
    * @param catalog
    *           system catalog of this foreign key
    * @param id
    *           globally unique ID of this foreign key
    * @param name
    *           name of this foreign key
    * @param columnID
    *           identifier of the constrained column
    * @param ownerID
    *           identifier of the owner table
    * @param columnIDs
    *           identifiers of the referencing columns
    * @param referencedTableID
    *           identifier of the table containing the referenced columns
    * @param referencedIDs
    *           identifiers of the referenced columns
    */
   private ForeignKeyDescriptor(final SystemCatalog catalog, final int id, final String name,
         final int columnID, final int ownerID, final int[] columnIDs, final int referencedTableID,
         final int[] referencedIDs) {
      super(catalog, id, name, columnID, ownerID);
      this.referencedTableID = referencedTableID;
      this.key = new CatalogForeignKey(new CatalogKey(columnIDs), new CatalogKey(referencedIDs));
   }

   /**
    * Returns the foreign key.
    *
    * @return foreign key
    */
   public CatalogForeignKey getKey() {
      return this.key;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toSQL() {
      final StringBuilder result = new StringBuilder();
      this.formatHeader(result);
      if (this.isTableConstraint()) {
         result.append("FOREIGN KEY ");
         this.formatKeyColumnNames(result, (CatalogKey) this.getKey().getReferencingColumns());
         result.append(" ");
      }
      result.append("REFERENCES ");
      final TableDescriptor table = this.getSystemCatalog().getTableDescriptor(this.referencedTableID);
      result.append(table.getName());
      this.formatKeyColumnNames(result, (CatalogKey) this.getKey().getReferencedColumns());
      return result.toString();
   }
}

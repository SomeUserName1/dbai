/*
 * @(#)IndexDescriptor.java   1.0   Jun 20, 2014
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
 * Descriptor for an index in the Minibase system catalog. An index descriptor provides read-only access to
 * the type of the index and its search key. Additionally, it records whether the index is clustered or not.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public class IndexDescriptor extends NamedDescriptor implements OwnedDescriptor {

   /** Statistics that describe the data in this index. */
   private final IndexStatistics statistics;

   /** Type of the index. */
   private final IndexType type;

   /** Indicates whether the index is clustered or not. */
   private final boolean clustered;

   /** Search key of the index. */
   private final CatalogSortKey key;

   /** Identifier of the owner table. */
   private final int ownerID;

   /**
    * Constructs a new index descriptor for an index with the given globally unique ID, name, type, and search
    * key. Additionally, an index can either be clustered or unclustered.
    *
    * @param catalog
    *           system catalog of the index
    * @param statistics
    *           statistics of the index
    * @param id
    *           globally unique ID of the index
    * @param name
    *           name of the index
    * @param type
    *           type of the index
    * @param clustered
    *           indicates whether the index is clustered or not
    * @param keyColumnIDs
    *           IDs of the search key columns of the index
    * @param keyOrders
    *           sort orders of the search key columns of the index
    * @param ownerID
    *           identifier of the owner table
    */
   public IndexDescriptor(final SystemCatalog catalog, final IndexStatistics statistics, final int id,
         final String name, final IndexType type, final boolean clustered, final int[] keyColumnIDs,
         final SortOrder[] keyOrders, final int ownerID) {
      super(catalog, id, name);
      this.statistics = statistics;
      this.type = type;
      this.clustered = clustered;
      this.key = new CatalogSortKey(keyColumnIDs, keyOrders);
      this.ownerID = ownerID;
   }

   /**
    * Returns a set of statistical values that characterize the data in the index described by this index
    * descriptor.
    *
    * @return index statistics
    */
   public IndexStatistics getStatistics() {
      return this.statistics;
   }

   /**
    * Returns the type of the index.
    *
    * @return type of the index
    */
   public IndexType getIndexType() {
      return this.type;
   }

   /**
    * Returns whether the index is clustered or not.
    *
    * @return {@code true} if the index is clustered, {@code false} otherwise
    */
   public boolean isClustered() {
      return this.clustered;
   }

   /**
    * Returns the search key of the index.
    *
    * @return search key of the index
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
      // TODO implement IndexDescriptor.toSQL
      return this.toString();
   }
}

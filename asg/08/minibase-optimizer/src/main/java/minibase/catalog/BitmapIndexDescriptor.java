/*
 * @(#)BitmapIndexDescriptor.java   1.0   Jun 27, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import minibase.query.optimizer.Expression;

/**
 * Descriptor for bitmap and bitmap join indices in the Minibase system catalog.
 * <p>
 * <em>This (transient) implementation of the system catalog exists purely for the development of the
 * Minibase query optimizer. Once the query optimizer has been fully implemented, it needs to be
 * integrated with the existing system catalog of Minibase. At that point, (most) of the classes in
 * this package can be removed.</em>
 * </p>
 * <p>
 * Minibase's query optimizer is based on the Cascades framework for query optimization and, additionally,
 * implements some of the improvements proposed by the Columbia database query optimizer.
 * <ul>
 * <li>Goetz Graefe: <strong>The Cascades Framework for Query Optimization</strong>. In
 * <em>IEEE Data(base) Engineering Bulletin</em>, 18(3), pp. 19-29, 1995.</li>
 * <li>Yongwen Xu: <strong>Efficiency in Columbia Database Query Optimizer</strong>,
 * <em>MSc Thesis, Portland State University</em>, 1998.</li>
 * </ul>
 * The Minibase query optimizer therefore descends from the EXODUS, Volcano, Cascades, and Columbia line of
 * query optimizers, which all use a rule-based, top-down approach to explore the space of possible query
 * execution plans, rather than a bottom-up approach based on dynamic programming.
 * </p>
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public final class BitmapIndexDescriptor extends IndexDescriptor {

   /** Columns referenced in the join condition used to build this index. */
   private final Set<CatalogKey> references;

   /** Predicate supported by this index. */
   private final Expression predicate;

   /**
    * Constructs a new index descriptor for a bitmap index with the given globally unique ID, name and search
    * key. This constructor sets the type of the index to {@link IndexType#BITMAP} and its clustered property
    * to {@code false}.
    *
    * @param catalog
    *           system catalog of the index
    * @param statistics
    *           statistics of the index
    * @param id
    *           globally unique ID of the index
    * @param name
    *           name of the index
    * @param keyColumnIDs
    *           IDs of the search key columns of the index
    * @param tableID
    *           ID of the owner table
    */
   public BitmapIndexDescriptor(final SystemCatalog catalog, final IndexStatistics statistics, final int id,
         final String name, final int[] keyColumnIDs, final int tableID) {
      super(catalog, statistics, id, name, IndexType.BITMAP, false, keyColumnIDs,
            keyOrders(keyColumnIDs.length), tableID);
      this.references = null;
      this.predicate = null;
   }

   /**
    * Constructs a new index descriptor for a bitmap join index with the given globally unique ID, name, and
    * search key. The described bitmap join index is built based on the given set of column references and
    * supports the evaluation of the given predicate. This constructor sets the type of the index to
    * {@link IndexType#BITMAP_JOIN} and its clustered property to {@code false}.
    *
    * @param catalog
    *           system catalog of the index
    * @param statistics
    *           statistics of the index
    * @param id
    *           globally unique ID of the index
    * @param name
    *           name of the index
    * @param keyColumnIDs
    *           IDs of the search key columns of the index
    * @param referencesIDs
    *           array of IDs of groups of referenced columns used to build the index
    * @param predicate
    *           predicate supported by the index
    * @param tableID
    *           ID of the owner table
    */
   public BitmapIndexDescriptor(final SystemCatalog catalog, final IndexStatistics statistics, final int id,
         final String name, final int[] keyColumnIDs, final int[][] referencesIDs,
         final Expression predicate, final int tableID) {
      super(catalog, statistics, id, name, IndexType.BITMAP_JOIN, false, keyColumnIDs,
            keyOrders(keyColumnIDs.length), tableID);
      this.references = new HashSet<>();
      for (final int[] referenceIDs : referencesIDs) {
         this.references.add(new CatalogKey(referenceIDs));
      }
      this.predicate = predicate;
   }

   /**
    * Returns the join condition used to create this index as a set of primary key/foreign key references.
    *
    * @return set of primary key/foreign key references
    */
   public Set<CatalogKey> getReferences() {
      return Collections.unmodifiableSet(this.references);
   }

   /**
    * Returns the predicate supported by this index as an expression tree.
    *
    * @return expression tree
    */
   public Expression getPredicate() {
      return this.predicate;
   }

   @Override
   public String toSQL() {
      // TODO implement BitmapIndexDescriptor.toSQL
      return this.toString();
   }

   /**
    * Initializes an array of the given length to {@link SortOrder#UNSORTED}.
    *
    * @param length
    *           array length
    * @return sort orders of this key
    */
   private static SortOrder[] keyOrders(final int length) {
      final SortOrder[] keyOrders = new SortOrder[length];
      Arrays.fill(keyOrders, SortOrder.UNSORTED);
      return keyOrders;
   }
}

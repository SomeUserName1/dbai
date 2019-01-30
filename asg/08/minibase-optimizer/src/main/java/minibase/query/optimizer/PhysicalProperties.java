/*
 * @(#)PhysicalProperties.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.Arrays;

import minibase.catalog.CatalogKey;
import minibase.catalog.DataOrder;
import minibase.catalog.SystemCatalog;
import minibase.query.schema.SchemaKey;

/**
 * Physical properties of an optimizer group.
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
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public class PhysicalProperties {

   /** Order of the data. i.e., ANY, HEAP, SORTED, or HASHED. */
   private final DataOrder order;

   /** Key on which data are ordered, or {@code null} if it is not ordered. */
   private SchemaKey key = null;

   /** Required columns in the projection list. */
   private final int[] projectColumnIDs;

   /**
    * Creates physical properties with the given data order and key.
    *
    * @param order
    *           order of the data
    * @param key
    *           key on which data are ordered, {@code null} if data order is ANY or HEAP
    * @param projectColumnIDs
    *           required columns in the projection list, {@code null} if all columns are projected
    */
   public PhysicalProperties(final DataOrder order, final SchemaKey key, final int[] projectColumnIDs) {
      if (key == null && (DataOrder.SORTED.equals(order) || DataOrder.HASHED.equals(order))) {
         throw new IllegalStateException("Key cannot be null for data order SORTED or HASHED.");
      }
      this.order = order;
      this.key = key;
      this.projectColumnIDs = projectColumnIDs;
   }

   /**
    * Creates physical properties with the given data order.
    *
    * @param order
    *           order of the data
    * @param projectColumnIDs
    *           required columns in the projection list, {@code null} if all columns are projected
    */
   public PhysicalProperties(final DataOrder order, final int[] projectColumnIDs) {
      this(order, null, projectColumnIDs);
   }

   /**
    * Returns the order of the data.
    *
    * @return data order
    */
   public DataOrder getOrder() {
      return this.order;
   }

   /**
    * Returns the key on which the data described by these properties are ordered.
    *
    * @return key on which data is is ordered, or {@code null} if it is not ordered
    */
   public SchemaKey getKey() {
      return this.key;
   }

   /**
    * Returns the IDs of required columns in the projection list.
    *
    * @return required columns in the projection list, {@code null} if all columns are projected
    */
   public int[] getProjectColumnID() {
      return this.projectColumnIDs == null ? null : Arrays.copyOf(this.projectColumnIDs,
            this.projectColumnIDs.length);
   }

   /**
    * Returns a new key that only contains the column with the maximal unique cardinality.
    *
    * @param catalog
    *           system catalog for cardinality lookups
    * @return best key
    */
   public CatalogKey getBestKey(final SystemCatalog catalog) {
      // TODO recheck this code snippet... it seems not to do for what it is intended to.
      // I commented it out because its not used anyway

      // double maxUniqueCardinality = -1;
      // int winner = -1;
      // for (int i = 0; i < this.key.size(); i++) {
      // final int columnID = this.key.getColumnID(i);
      // final double uniqueCardinality = catalog.getColumnDescriptor(columnID).getStatistics()
      // .getUniqueCardinality();
      // maxUniqueCardinality = Math.max(maxUniqueCardinality, uniqueCardinality);
      // if (uniqueCardinality == maxUniqueCardinality) {
      // winner = 0;
      // }
      // }
      // if (DataOrder.SORTED.equals(this.order)) {
      // return new IndexKey(new int[] { this.key.getColumnID(winner) },
      // new SortOrder[] { ((IndexKey) this.key).getOrder(winner) });
      // }
      // return new Key(new int[] { this.key.getColumnID(winner) });
      return null;
   }

   /**
    * Merges the given physical properties into these physical properties.
    *
    * @param other
    *           physical properties
    */
   public void merge(final PhysicalProperties other) {
      // TODO Implement this method.
      throw new OptimizerError("Method not implemented: PhysicalProperties#merge(PhysicalProperties)");
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int hashCode() {
      // TODO Implement this method.
      throw new OptimizerError("Method not implemented: PhysicalProperties#hashCode()");
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean equals(final Object other) {
      if (this == other) {
         return true;
      }
      if (other == null) {
         return false;
      }
      if (this.getClass() != other.getClass()) {
         return false;
      }
      final PhysicalProperties physicalProperties = (PhysicalProperties) other;
      if (DataOrder.ANY.equals(this.order) && DataOrder.ANY.equals(physicalProperties.order)) {
         return true;
      }
      if (DataOrder.ANY.equals(this.order) || DataOrder.ANY.equals(physicalProperties.order)) {
         return false;
      }
      if (this.order.equals(physicalProperties.order) && this.key.equals(physicalProperties.key)) {
         return true;
      }
      return false;
   }

   /** Reference to physical properties without requirements, i.e., any data order. */
   private static final PhysicalProperties ANY_PROPERTIES = new PhysicalProperties(DataOrder.ANY, null);

   /**
    * Returns physical properties that do not impose any requirements, i.e., can have any data order.
    *
    * @return physical properties
    */
   public static PhysicalProperties anyPhysicalProperties() {
      return ANY_PROPERTIES;
   }
}

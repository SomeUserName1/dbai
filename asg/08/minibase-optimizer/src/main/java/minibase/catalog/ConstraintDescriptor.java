/*
 * @(#)ConstraintDescriptor.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.stream.Collectors;

/**
 * Common abstract base class for constraint descriptors managed by the Minibase system catalog.
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
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public abstract class ConstraintDescriptor extends NamedDescriptor implements OwnedDescriptor {

   /** Constrained column identifier or {@link SystemCatalog#INVALID_ID} if this is a table constraint. */
   private final int columnID;

   /** Identifier of the owning table. */
   private final int ownerID;

   /**
    * Constructs a new constraint descriptor with the given globally unique ID, name, and parent ID.
    *
    * @param catalog
    *           system catalog of this constraint
    * @param catalogID
    *           identifier of this constraint
    * @param name
    *           name of this constraint
    * @param columnID
    *           Identifier of the constrained column or {@link SystemCatalog#INVALID_ID} if this is a table
    *           constraint
    * @param ownerID
    *           identifier of the owning table
    */
   public ConstraintDescriptor(final SystemCatalog catalog, final int catalogID, final String name,
         final int columnID, final int ownerID) {
      super(catalog, catalogID, name);
      this.columnID = columnID;
      this.ownerID = ownerID;
   }

   /**
    * Returns whether this constraint is a table constraint. If {@code true} the ID returned by
    * {@link #getParentID()} points to a table, otherwise it points to an attribute.
    *
    * @return {@code true} if this constraint is a table constraint, {@code false} otherwise.
    */
   public boolean isTableConstraint() {
      return this.columnID == SystemCatalog.INVALID_ID;
   }

   /**
    * Returns the identifier of the column that is constraint or {@link SystemCatalog#INVALID_ID}, if this is
    * a table constraint.
    *
    * @return column identifier
    */
   public int getColumnID() {
      return this.columnID;
   }

   @Override
   public int getOwnerID() {
      return this.ownerID;
   }

   /**
    * Appends the list of column names to the given string buffer.
    *
    * @param result
    *           string buffer to which the list of column names is appended
    * @param columns
    *           columns to append
    */
   void formatKeyColumnNames(final StringBuilder result, final CatalogKey columns) {
      result.append("(");
      result.append(columns.stream()
            .map(colId -> this.getSystemCatalog().getColumnDescriptor(colId).getName())
            .collect(Collectors.joining(", ")));
      result.append(")");
   }

   /**
    * Appends the constraint header to the given string buffer.
    *
    * @param result
    *           string buffer to which the constraint header is appended
    */
   void formatHeader(final StringBuilder result) {
      if (!this.isNameInternal()) {
         result.append("CONSTRAINT ");
         result.append(this.getName());
         result.append(" ");
      }
   }
}

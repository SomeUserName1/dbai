/*
 * @(#)AliasedColumnReference.java   1.0   Jun 20, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import minibase.catalog.DataType;

/**
 * A column reference that is derived by aliasing/renaming.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @since 1.0
 */
public class AliasedColumnReference extends AbstractReference implements DerivedColumnReference {

   /** Name of this aliased column. */
   private final String name;

   /** Original column of this aliased column. */
   private final ColumnReference original;

   /**
    * Creates a new aliased column with the given name for the given column.
    *
    * @param name
    *           column name
    * @param original
    *           original column
    */
   AliasedColumnReference(final String name, final ColumnReference original) {
      super(original.getID());
      this.name = name;
      this.original = original;
   }

   @Override
   public DataType getType() {
      return this.original.getType();
   }

   @Override
   public Optional<TableReference> getParent() {
      return this.original.getParent();
   }

   @Override
   public String getName() {
      return this.name;
   }

   @Override
   public int getSize() {
      return this.original.getSize();
   }

   @Override
   public double getWidth() {
      return this.original.getWidth();
   }

   @Override
   public List<ColumnReference> getInputColumns() {
      return Collections.singletonList(this.original);
   }

   @Override
   public boolean isAggregation() {
      if (this.original instanceof DerivedColumnReference) {
         return ((DerivedColumnReference) this.original).isAggregation();
      }
      return false;
   }

   @Override
   public String toString() {
      return this.name;
   }
}

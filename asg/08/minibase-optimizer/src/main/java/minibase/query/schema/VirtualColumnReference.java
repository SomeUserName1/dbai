/*
 * @(#)VirtualColumnReference.java   1.0   Feb 26, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

import java.util.List;
import java.util.Optional;

import minibase.catalog.DataType;

/**
 * A virtual derived column reference.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public class VirtualColumnReference extends AbstractReference implements DerivedColumnReference {

   /** Name of this virtual derived column. */
   private final String name;

   /** Declared data type of this virtual derived column. */
   private final DataType type;

   /** Declared size of this virtual derived column in bytes. */
   private final int size;

   /** Declared width of this virtual derived column as a fraction of a block. */
   private final double width;

   /** Input columns of this virtual derived column. */
   private final List<ColumnReference> inputs;

   /** Indicates whether this virtual derived column is an aggregated column. */
   private final boolean aggregation;

   /**
    * Constructs a new reference to the given expression with the given reference ID and name.
    *
    * @param id
    *           ID of this virtual derived column
    * @param name
    *           name of this virtual derived column
    * @param type
    *           declared type of this virtual derived column
    * @param size
    *           declared size of this virtual derived column
    * @param width
    *           declared width of this virtual derived column
    * @param inputs
    *           input columns of this virtual derived column
    * @param aggregation
    *           indicates whether this virtual derived column is aggregated
    */
   VirtualColumnReference(final int id, final String name, final DataType type, final int size,
         final double width, final List<ColumnReference> inputs, final boolean aggregation) {
      super(id);
      this.name = name;
      this.type = type;
      this.size = size;
      this.width = width;
      this.inputs = inputs;
      this.aggregation = aggregation;
   }

   @Override
   public Optional<TableReference> getParent() {
      return Optional.empty();
   }

   @Override
   public String getName() {
      return this.name;
   }

   @Override
   public DataType getType() {
      return this.type;
   }

   @Override
   public int getSize() {
      return this.size;
   }

   @Override
   public double getWidth() {
      return this.width;
   }

   @Override
   public List<ColumnReference> getInputColumns() {
      return this.inputs;
   }

   @Override
   public boolean isAggregation() {
      return this.aggregation;
   }

   @Override
   public String toString() {
      return this.getName();
   }
}

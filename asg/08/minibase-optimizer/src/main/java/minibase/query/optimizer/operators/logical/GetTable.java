/*
 * @(#)GetTable.java   1.0   Jan 3, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import minibase.catalog.CatalogForeignKey;
import minibase.catalog.CatalogKey;
import minibase.catalog.ColumnStatistics;
import minibase.catalog.ForeignKeyDescriptor;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.JenkinsHash;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaForeignKey;
import minibase.query.schema.SchemaKey;
import minibase.query.schema.StoredColumnReference;
import minibase.query.schema.StoredTableReference;

/**
 * The {@code GetTable} logical operator reads a table that is stored on disk. As it is therefore a leaf in
 * any logical expression, it has no inputs and arity is 0. The data to be loaded is specified by the
 * corresponding table name and, optionally, a table alias, such as {@code S} in the
 * {@code SELECT S.sid FROM Sailors <strong>AS S</strong>;} SQL statement. Because this operator is a leaf of
 * the query tree, its caches its logical properties as they are not assumed to change during query
 * optimization.
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
public class GetTable extends AbstractLogicalOperator {

   /** Reference to the table. */
   private final StoredTableReference table;

   /** Table logical properties of this operator. */
   private final LogicalCollectionProperties properties;

   /** Schema of this operator. */
   private final Schema schema;

   /**
    * Constructs a new {@code GetTable} logical operator with the given table reference.
    *
    * @param table
    *           table scanned by this operator
    */
   public GetTable(final StoredTableReference table) {
      super(OperatorType.GETTABLE);
      this.table = table;
      // Build schema and column statistics.
      final List<ColumnReference> columns = table.getColumns();
      final List<ColumnStatistics> statistics = new ArrayList<>();
      for (final ColumnReference column : columns) {
         statistics.add(((StoredColumnReference) column).getDescriptor().getStatistics());
      }
      // Convert catalog to schema keys
      final List<SchemaKey> candidateKeys = new ArrayList<>();
      for (final CatalogKey candidateKey : table.getDescriptor().getCandidateKeys()) {
         candidateKeys.add(SchemaKey.resolve(candidateKey, columns).get());
      }
      // Convert catalog to schema foreign keys
      final List<SchemaForeignKey> foreignKeys = new ArrayList<>();
      for (final ForeignKeyDescriptor foreignKeyDesc : table.getDescriptor().getForeignKeys()) {
         final CatalogForeignKey fk = foreignKeyDesc.getKey();
         foreignKeys.add(new SchemaForeignKey(SchemaKey.resolve(fk.getReferencingColumns(), columns).get(),
               fk.getReferencedColumns()));
      }
      // Create and set the schema of this operator
      this.schema = new Schema(columns, Collections.singletonList(this.table), candidateKeys, foreignKeys);
      this.properties = new LogicalCollectionProperties(this.schema, table.getDescriptor().getStatistics(),
            statistics, true);
   }

   /**
    * Constructs a new {@code GetTable} logical operator. This variant of the constructor is used by the rule
    * engine to create a template of a get-table operator. Therefore, all its fields are initialized to
    * {@code null}.
    */
   public GetTable() {
      super(OperatorType.GETTABLE);
      this.table = null;
      this.schema = null;
      this.properties = null;
   }

   /**
    * Returns the reference to the table accessed by this operator.
    *
    * @return table reference
    */
   public StoredTableReference getTable() {
      return this.table;
   }

   @Override
   public Schema getSchema(final Schema... inputSchemas) {
      return this.schema;
   }

   @Override
   public LogicalCollectionProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      if (inputProperties != null && inputProperties.length > 0) {
         throw new IllegalArgumentException("Opertor " + this.getName() + " cannot have input properties.");
      }
      return this.properties;
   }

   @Override
   public int hashCode() {
      return JenkinsHash.lookup2(this.table.getID(), super.hashCode());
   }

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
      final GetTable opertator = (GetTable) other;
      return this.table.getID() == opertator.table.getID();
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      result.append(this.table.getName());
      result.append("]");
      return result.toString();
   }
}

/*
 * @(#)EquiJoin.java   1.0   Feb 14, 2014
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.TableStatistics;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.StatisticalModel;
import minibase.query.optimizer.operators.OperatorType;
import minibase.query.optimizer.util.JenkinsHash;
import minibase.query.optimizer.util.StrongReference;
import minibase.query.schema.ColumnReference;
import minibase.query.schema.JoinMode;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaForeignKey;
import minibase.query.schema.SchemaKey;
import minibase.query.schema.TableReference;

/**
 * The {@code EquiJoin} logical operator joins two inputs using equality predicates over pairs of attributes.
 * If no equality predicates are given this equi-join operator performs a natural join.
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
public class EquiJoin extends AbstractLogicalOperator {

   /** Reference IDs of left columns. */
   private final List<ColumnReference> leftColumns;

   /** Reference IDs of right columns. */
   private final List<ColumnReference> rightColumns;

   /** Join mode of this equi-join. */
   private JoinMode mode;

   /** Foreign key relations direction. */
   private boolean leftToRight = true;

   /** Foreign key used by this join, if any. */
   private StrongReference<SchemaForeignKey> foreignKey = new StrongReference<>();

   /**
    * Constructs a new {@code EquiJoin} logical operator. The join predicate is given in the form of two sets
    * of the same size. Each set contains a number of references to columns in the system catalog. Columns are
    * assumed to be pair-wise equal, i.e., the join predicate is {@code leftColumns[1] = rightColumns[1] AND}
    * ... {@code AND leftColumns[n] = rightColumns[n]}. If both sets of column reference IDs are empty, a
    * natural join or a cross-product is performed.
    *
    * @param leftColumns
    *           references to left columns
    * @param rightColumns
    *           references to right columns
    */
   public EquiJoin(final List<ColumnReference> leftColumns, final List<ColumnReference> rightColumns) {
      super(OperatorType.EQJOIN);
      if (leftColumns.size() != rightColumns.size()) {
         throw new IllegalArgumentException("Number of left join columns (" + leftColumns.size()
               + ") does not match number of right join columns (" + rightColumns.size() + ").");
      }
      this.leftColumns = new ArrayList<>();
      this.rightColumns = new ArrayList<>();
      if (leftColumns.size() > 1) {
         final List<EqualityPair> pairs = new ArrayList<>();
         for (int i = 0; i < leftColumns.size(); i++) {
            final EqualityPair pair = new EqualityPair(leftColumns.get(i), rightColumns.get(i));
            pairs.add(pair);
         }
         Collections.sort(pairs);
         for (final EqualityPair pair : pairs) {
            this.leftColumns.add(pair.getLeft());
            this.rightColumns.add(pair.getRight());
         }
      } else {
         this.leftColumns.addAll(leftColumns);
         this.rightColumns.addAll(rightColumns);
      }
   }

   /**
    * Constructs a new {@code EquiJoin} logical operator. This variant of the constructor is used by the rule
    * engine to create a template of a join operator. Therefore, all its fields are initialized to
    * {@code null}.
    */
   public EquiJoin() {
      super(OperatorType.EQJOIN);
      this.leftColumns = null;
      this.rightColumns = null;
   }

   /**
    * Returns an unmodifiable list containing the references to the join columns of the left input.
    *
    * @return join column references
    */
   public List<ColumnReference> getLeftColumns() {
      return this.leftColumns;
   }

   /**
    * Returns an unmodifiable list containing the references to the join columns of the right input.
    *
    * @return join column references
    */
   public List<ColumnReference> getRightColumns() {
      return this.rightColumns;
   }

   @Override
   public boolean isCommuting() {
      return true;
   }

   @Override
   public LogicalCollectionProperties getLogicalProperties(final LogicalProperties... inputProperties) {
      if (inputProperties.length != this.getArity()) {
         throw new IllegalArgumentException("Number of input logical properties (" + inputProperties.length
               + ") does not match operator arity (" + this.getArity() + ").");
      }
      final LogicalCollectionProperties leftProperties = (LogicalCollectionProperties) inputProperties[0];
      final LogicalCollectionProperties rightProperties = (LogicalCollectionProperties) inputProperties[1];
      final Schema leftSchema = leftProperties.getSchema();
      final Schema rightSchema = rightProperties.getSchema();
      final Schema schema = this.getSchema(leftSchema, rightSchema);

      // TODO: The following assertions are violated due to missing statistics in the system catalog.
      // check cardinalities of left and right input
      // assert leftProperties.getCardinality() >= 0 : "Left input cardinality < 0";
      // assert leftProperties.getUniqueCardinality() >= 0 : "Left input unique cardinality < 0";
      // assert rightProperties.getCardinality() >= 0 : "Right input cardinality < 0";
      // assert rightProperties.getUniqueCardinality() >= 0 : "Right input unique cardinality < 0";

      // check that the attributes of the join predicates are in the schema
      for (int i = 0; i < this.leftColumns.size(); i++) {
         final ColumnReference reference = this.leftColumns.get(i);
         assert leftSchema.containsColumn(reference) : "Left schema does not contain join column";
      }
      for (int i = 0; i < this.rightColumns.size(); i++) {
         final ColumnReference reference = this.rightColumns.get(i);
         assert rightSchema.containsColumn(reference) : "Right schema does not contain join column";
      }

      // compute table logical properties of natural join: the cardinality of an equi-join is the cardinality
      // of referenced key input (as opposed to foreign key input), divided by refUniqueCardinality

      final double joinCard;
      final double joinUCard;
      final double leftCard = leftProperties.getCardinality();
      final double leftUCard = leftProperties.getUniqueCardinality();
      final double rightCard = rightProperties.getCardinality();
      final double rightUCard = rightProperties.getUniqueCardinality();
      if (this.leftColumns.size() > 0) {
         // join
         switch (this.mode) {
            case FULL_FK:
               if (this.leftToRight) {
                  joinCard = leftCard;
                  joinUCard = leftUCard;
               } else {
                  joinCard = rightCard;
                  joinUCard = rightUCard;
               }
               break;
            case SOURCE_FK:
               final SchemaForeignKey fk = this.foreignKey.get();
               int refUCard = 1;
               for (final ColumnReference column : fk.getReferencedColumns()) {
                  final ColumnStatistics statistics;
                  if (this.leftToRight) {
                     final int index = rightSchema.getColumnIndex(column);
                     statistics = rightProperties.getColumnStatistics().get(index);
                  } else {
                     final int index = leftSchema.getColumnIndex(column);
                     statistics = leftProperties.getColumnStatistics().get(index);
                  }
                  refUCard *= statistics.getUniqueCardinality();
               }
               if (this.leftToRight) {
                  joinCard = leftCard * (rightCard / refUCard);
                  joinUCard = leftUCard * (rightCard / refUCard);
               } else {
                  joinCard = rightCard * (leftCard / refUCard);
                  joinUCard = rightUCard * (leftCard / refUCard);
               }
               break;
            case GENERAL:
               final double selectivity = Math.pow(StatisticalModel.EQUALITY_SELECTIVITY.getSelectivity(),
                     this.leftColumns.size());
               joinCard = leftCard * rightCard * selectivity;
               joinUCard = leftUCard * rightUCard * selectivity;
               break;
            default:
               throw new IllegalStateException(
                     "Encountered an unkown equi-join mode: " + this.mode.toString() + ".");
         }
      } else {
         // cross-product
         joinCard = leftCard * rightCard;
         joinUCard = leftUCard * rightUCard;
      }

      // derive new column statistics and create new table statistics
      double width = 0.0;
      final List<ColumnStatistics> columnStatistics = new ArrayList<>();
      for (final ColumnStatistics s : leftProperties.getColumnStatistics()) {
         final ColumnStatistics statistics = deriveStatistics(s);
         width += statistics.getWidth();
         columnStatistics.add(statistics);
      }
      for (final ColumnStatistics s : rightProperties.getColumnStatistics()) {
         final ColumnStatistics statistics = deriveStatistics(s);
         width += statistics.getWidth();
         columnStatistics.add(statistics);
      }
      final TableStatistics tableStatistics = new TableStatistics(joinCard, joinUCard, width);

      return new LogicalCollectionProperties(schema, tableStatistics, columnStatistics, false);
   }

   @Override
   public Schema getSchema(final Schema... inputSchemas) {
      return this.join(inputSchemas[0], inputSchemas[1]);
   }

   /**
    * Returns the joined schema of the given left and right schema.
    *
    * @param leftSchema
    *           left schema
    * @param rightSchema
    *           right schema
    * @return joined schema
    */
   private Schema join(final Schema leftSchema, final Schema rightSchema) {
      // Try to find a foreign key relationship: left -> right
      this.mode = findJoinMode(this.foreignKey, leftSchema, rightSchema);
      // check if a full foreign key relationship was found
      if (this.mode.getPriority() > JoinMode.FULL_FK.getPriority()) {
         // try to find a foreign key relationship: right -> left
         final StrongReference<SchemaForeignKey> foreignKeyRL = new StrongReference<>();
         final JoinMode modeRL = findJoinMode(foreignKeyRL, rightSchema, leftSchema);
         if (modeRL.getPriority() < this.mode.getPriority()) {
            this.mode = modeRL;
            this.foreignKey = foreignKeyRL;
            this.leftToRight = false;
         }
      }
      final List<TableReference> tables = new ArrayList<>();
      // Add all tables from this schema.
      for (int i = 0; i < leftSchema.getTableCount(); i++) {
         tables.add(leftSchema.getTable(i));
      }
      // Add all tables from the other schema, if they are not already present.
      for (int i = 0; i < rightSchema.getTableCount(); i++) {
         final TableReference tableRef = rightSchema.getTable(i);
         if (!leftSchema.containsTable(tableRef)) {
            tables.add(tableRef);
         }
      }
      final List<ColumnReference> columns = new ArrayList<>();
      // Add all columns and their (derived) statistics from this schema.
      for (int i = 0; i < leftSchema.getColumnCount(); i++) {
         columns.add(leftSchema.getColumn(i));
      }
      // Add all columns and their (derived) statistics from the other schema.
      for (int i = 0; i < rightSchema.getColumnCount(); i++) {
         columns.add(rightSchema.getColumn(i));
      }
      // Compute candidate keys based on the join mode.
      Collection<SchemaKey> candidateKeys = computeUnionedKeyCandidates(leftSchema, rightSchema);
      if (JoinMode.FULL_FK.equals(this.mode)) {
         if (this.leftToRight) {
            candidateKeys = leftSchema.getCandidateKeys();
         } else {
            candidateKeys = rightSchema.getCandidateKeys();
         }
      }

      final List<SchemaForeignKey> foreignKeys = new ArrayList<>();
      // foreign key is the union of the left and the right foreign key
      foreignKeys.addAll(leftSchema.getForeignKeys());
      foreignKeys.addAll(rightSchema.getForeignKeys());
      return new Schema(columns, tables, candidateKeys, foreignKeys);
   }

   /**
    * Checks whether there is a foreign key relationship between the two given sets of column references,
    * based on the given source and target properties.
    *
    * @param foreignKey
    *           reference parameter to return the found foreign key, if any
    * @param sourceSchema
    *           schema of the source input
    * @param targetSchema
    *           schema of the target input
    * @return equi-join mode
    */
   private static JoinMode findJoinMode(final StrongReference<SchemaForeignKey> foreignKey,
         final Schema sourceSchema, final Schema targetSchema) {
      JoinMode mode = JoinMode.GENERAL;
      // try to match all foreign keys of source input to referenced key of target input
      for (final SchemaForeignKey fk : sourceSchema.getForeignKeys()) {
         // check if source column references contain the foreign key
         if (fk.getReferencingColumns().isSubsetOf(sourceSchema)) {
            // check if target column references contain the corresponding referenced key
            if (!fk.isResolved()) {
               // Foreign key is not yet resolved: attempt to resolve it against the target schema
               fk.resolve(targetSchema);
            }
            if (fk.isResolved() && fk.getReferencedColumns().isSubsetOf(targetSchema)) {
               // check if referenced key is a candidate key of the target input
               // TODO Check the next line: using equals seems too restrictive.
               if (targetSchema.getCandidateKeys().contains(fk.getReferencedColumns())) {
                  return JoinMode.FULL_FK;
               } else {
                  mode = JoinMode.SOURCE_FK;
                  foreignKey.set(fk);
               }
            }
         }
      }
      return mode;
   }

   /**
    * The Cartesian product of the candidate keys; Each pair of keys is merged to one key.
    *
    * @param leftSchema
    *           left schema
    * @param rightSchema
    *           right schema
    * @return Cartesian product of the candidate keys
    */
   private static Collection<SchemaKey> computeUnionedKeyCandidates(final Schema leftSchema,
         final Schema rightSchema) {
      final Set<SchemaKey> candKeys = new HashSet<>();
      final Collection<SchemaKey> ks2 = rightSchema.getCandidateKeys();
      for (final SchemaKey k1 : leftSchema.getCandidateKeys()) {
         for (final SchemaKey k2 : ks2) {
            candKeys.add(k1.union(k2));
         }
      }
      return candKeys;
   }

   /**
    * Derives new statistics from the given statistics. The algorithm is taken from the Cascades framework,
    * which simply takes over the values for minimum and maximum, and divides the column unique cardinality by
    * two.
    *
    * @param original
    *           original statistics
    * @return derived statistics
    */
   private static ColumnStatistics deriveStatistics(final ColumnStatistics original) {
      double uniqueCardinality = original.getUniqueCardinality();
      uniqueCardinality = uniqueCardinality != -1 ? uniqueCardinality / 2 : -1;
      return new ColumnStatistics(original.getCardinality(), uniqueCardinality, original.getMinimum(),
            original.getMaximum(), original.getWidth());
   }

   @Override
   public int hashCode() {
      return this.hashCode(false);
   }

   @Override
   public int hashCode(final boolean ignoreInputOrder) {
      int hashCode = super.hashCode();
      if (this.leftColumns != null && this.rightColumns != null) {
         if (this.leftColumns.size() > 0) {
            List<ColumnReference> left = this.leftColumns;
            List<ColumnReference> right = this.rightColumns;
            // Bring left and right into "canonical" order based on the ID of the first reference.
            if (ignoreInputOrder && left.get(0).getID() > right.get(0).getID()) {
               left = this.rightColumns;
               right = this.leftColumns;
            }
            for (int i = left.size() - 1; i >= 0; i--) {
               hashCode = JenkinsHash.lookup2(left.get(i).hashCode(), hashCode);
               hashCode = JenkinsHash.lookup2(right.get(i).hashCode(), hashCode);
            }
         }
      } else {
         throw new IllegalStateException("Equi-join column references cannot be null.");
      }
      return hashCode;
   }

   @Override
   public boolean equals(final Object other) {
      return this.equals(other, false);
   }

   @Override
   public boolean equals(final Object other, final boolean ignoreInputOrder) {
      if (this == other) {
         return true;
      }
      if (other == null) {
         return false;
      }
      if (this.getClass() != other.getClass()) {
         return false;
      }
      final EquiJoin equijoin = (EquiJoin) other;
      if (this.leftColumns.size() == 0 && equijoin.leftColumns.size() == 0) {
         return true;
      }
      if (this.leftColumns.size() == equijoin.leftColumns.size()) {
         // Try original predicate for exact match
         if (this.leftColumns.equals(equijoin.leftColumns)
               && this.rightColumns.equals(equijoin.rightColumns)) {
            return true;
         } else if (ignoreInputOrder) {
            // Try commuted predicate for equivalent match
            return this.leftColumns.equals(equijoin.rightColumns)
                  && this.rightColumns.equals(equijoin.leftColumns);
         }
      }
      return false;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer(this.getName());
      result.append("[");
      for (int i = 0; i < this.leftColumns.size(); i++) {
         result.append(this.leftColumns.get(i).getName());
         result.append("=");
         result.append(this.rightColumns.get(i).getName());
         if (i + 1 < this.leftColumns.size()) {
            result.append(", ");
         }
      }
      result.append("]");
      return result.toString();
   }

   /**
    * Helper class to sort predicates initially.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private final class EqualityPair implements Comparable<EqualityPair> {

      /** Left hand side of the comparison. */
      private final ColumnReference lhs;
      /** Right hand side of the comparison. */
      private final ColumnReference rhs;

      /**
       * Creates a new equality pair.
       *
       * @param lhs
       *           left hand side
       * @param rhs
       *           right hand side
       */
      private EqualityPair(final ColumnReference lhs, final ColumnReference rhs) {
         this.lhs = lhs;
         this.rhs = rhs;
      }

      /**
       * Returns the left hand side of this equality pair.
       *
       * @return left hand side
       */
      private ColumnReference getLeft() {
         return this.lhs;
      }

      /**
       * Returns the right hand side of this equality pair.
       *
       * @return right hand side
       */
      private ColumnReference getRight() {
         return this.rhs;
      }

      @Override
      public int compareTo(final EqualityPair pair) {
         if (this.lhs.getID() > pair.lhs.getID()) {
            return 1;
         } else if (this.lhs.getID() < pair.lhs.getID()) {
            return -1;
         } else {
            return Integer.compare(this.rhs.getID(), pair.rhs.getID());
         }
      }
   }
}

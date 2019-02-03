/*
 * @(#)SystemCatalogSchema.java   1.0   Aug 24, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import minibase.query.schema.Schema;
import minibase.query.schema.SchemaBuilder;
import minibase.storage.file.DiskManager;

/**
 * Schema of the persistent Minibase system catalog, which defines the following tables to manage tables,
 * columns, indexes, and statistics.
 *
 * <pre>
 * systables(tabid, name, order, card, idctr)
 * syscolumns(colid, tabid, colno, name, type, size, card, ucard, min, max)
 * sysconstraints(constid, tabid, colid, name, type)
 * sysprimarykeys(constid, tabid, colid, keyno)
 * sysforeignkeys(constid, fkeyid, rkeyid, fkey, rkey, keyno)
 * sysindexes(idxid, tabid, name, type, clustered)
 * sysindexkeys(idxid, tabid, colid, order, keyno)
 * </pre>
 *
 * The schema of the Minibase system catalog is inspired by the system tables of Microsoft SQL Server 2000
 * (http://technet.microsoft.com/en-us/library/aa260604) and designed to meet the requirements of the Cascades
 * query optimizer framework.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
final class SystemCatalogSchema {

   /** Prefix of all system catalog tables. */
   private static final String SYS_PREFIX = "sys";

   /** Enumeration of all tables defined by the Minibase system catalog. */
   enum Table {
      /** Tables table. */
      TABLES(SYS_PREFIX + "tables"),
      /** Columns table. */
      COLUMNS(SYS_PREFIX + "columns"),
      /** Constraints table. */
      CONSTRAINTS(SYS_PREFIX + "constraints"),
      /** Primary keys table. */
      PRIMARYKEYS(SYS_PREFIX + "primarykeys"),
      /** Foreign keys table. */
      FOREIGNKEYS(SYS_PREFIX + "foreignkeys"),
      /** Indexes table. */
      INDEXES(SYS_PREFIX + "indexes"),
      /** Index keys table. */
      INDEXKEYS(SYS_PREFIX + "indexkeys");

      /** Name of the table. */
      private String name;

      /**
       * Creates a new table enumeration entry with the given name.
       *
       * @param name
       *           table name
       */
      Table(final String name) {
         this.name = name;
      }

      /**
       * Returns the name of this table.
       *
       * @return table name
       */
      public String getName() {
         return this.name;
      }
   }

   /** Enumeration of all columns defined by the Minibase system catalog. */
   enum Column {
      // systables(tabid, name, order, idctr, card)
      /** Tables table identifier column. */
      TABLES_TABID(0, DataType.INT, "tabid"),
      /** Tables table name column. */
      TABLES_NAME(1, DataType.CHAR, DiskManager.NAME_MAXLEN, "name"),
      /** Tables table order column. */
      TABLES_ORDER(2, DataType.INT, "order"),
      /** Tables table cardinality column. */
      TABLES_CARD(3, DataType.INT, "card"),
      /** Tables table identifier counter column. */
      TABLES_IDCTR(4, DataType.INT, "idctr"),
      // syscolumns(colid, name, tabid, type, size, card, ucard, min, max)
      /** Columns table identifier column. */
      COLUMNS_COLID(0, DataType.INT, "colid"),
      /** Columns table table identifier column. */
      COLUMNS_TABID(1, DataType.INT, "tabid"),
      /** Columns table column position column. */
      COLUMNS_COLNO(2, DataType.INT, "colno"),
      /** Columns table name attribute. */
      COLUMNS_NAME(3, DataType.CHAR, DiskManager.NAME_MAXLEN, "name"),
      /** Columns table type column. */
      COLUMNS_TYPE(4, DataType.INT, "type"),
      /** Columns table size column. */
      COLUMNS_SIZE(5, DataType.INT, "size"),
      /** Columns table cardinality column. */
      COLUMNS_CARD(6, DataType.INT, "card"),
      /** Columns table unique cardinality column. */
      COLUMNS_UCARD(7, DataType.INT, "ucard"),
      /** Columns table maximum value column. */
      COLUMNS_MIN(8, DataType.FLOAT, "min"),
      /** Columns table minimum value column. */
      COLUMNS_MAX(9, DataType.FLOAT, "max"),
      // sysconstraints(constid, name, tabid, colid, type)
      /** Constraints table constraint identifier column. */
      CONSTRAINTS_CONSTID(0, DataType.INT, "constid"),
      /** Constraints table name column. */
      CONSTRAINTS_TABID(1, DataType.INT, "tabid"),
      /** Constraints table column identifier column. */
      CONSTRAINTS_COLID(2, DataType.INT, "colid"),
      /** Constraints table type column. */
      CONSTRAINTS_NAME(3, DataType.CHAR, DiskManager.NAME_MAXLEN, "name"),
      /** Constraints table table identifier column. */
      CONSTRAINTS_TYPE(4, DataType.INT, "type"),
      // sysprimarykeys(constid, tabid, colid, keyno)
      /** Primary keys table constraint identifier column. */
      PRIMARYKEYS_CONSTID(0, DataType.INT, "constid"),
      /** Primary keys table table identifier column. */
      PRIMARYKEYS_TABID(1, DataType.INT, "tabid"),
      /** Primary keys table column identifier column. */
      PRIMARYKEYS_COLID(2, DataType.INT, "colid"),
      /** Primary keys table column position column. */
      PRIMARYKEYS_KEYNO(3, DataType.INT, "keyno"),
      // sysforeignkeys(constid, fkeyid, rkeyid, fkey, rkey, keyno)
      /** Foreign keys table constraint identifier column. */
      FOREIGNKEYS_CONSTID(0, DataType.INT, "constid"),
      /** Foreign keys table foreign key identifier column. */
      FOREIGNKEYS_FKEYID(1, DataType.INT, "fkeyid"),
      /** Foreign keys table referenced key identifier column. */
      FOREIGNKEYS_RKEYID(2, DataType.INT, "rkeyid"),
      /** Foreign keys table referencing column identifier column. */
      FOREIGNKEYS_FKEY(3, DataType.INT, "fkey"),
      /** Foreign keys table referenced column identifier column. */
      FOREIGNKEYS_RKEY(4, DataType.INT, "rkey"),
      /** Foreign keys table key column position column. */
      FOREIGNKEYS_KEYNO(5, DataType.INT, "keyno"),
      // sysindexes(idxid, tabid, name, type, clustered)
      /** Indexes table index identifier column. */
      INDEXES_IDXID(0, DataType.INT, "idxid"),
      /** Indexes table table identifier column. */
      INDEXES_TABID(1, DataType.INT, "tabid"),
      /** Indexes table name column. */
      INDEXES_NAME(2, DataType.CHAR, DiskManager.NAME_MAXLEN, "name"),
      /** Indexes table clustered column. */
      INDEXES_TYPE(3, DataType.INT, "type"),
      /** Indexes table clustered column. */
      INDEXES_CLUSTERED(4, DataType.INT, "clustered"),
      // sysindexkeys(idxid, tabid, colid, order, keyno)
      /** Index keys table index identifier column. */
      INDEXKEYS_IDXID(0, DataType.INT, "idxid"),
      /** Index keys table table identifier column. */
      INDEXKEYS_TABID(1, DataType.INT, "tabid"),
      /** Index keys table column identifier column. */
      INDEXKEYS_COLID(2, DataType.INT, "colid"),
      /** Index keys table column order column. */
      INDEXKEYS_ORDER(3, DataType.INT, "order"),
      /** Index keys table key column position column. */
      INDEXKEYS_KEYNO(4, DataType.INT, "keyno");

      /** Position of the column. */
      private int pos;

      /** Type of the column. */
      private DataType type;

      /** Size of the column. */
      private int size;

      /** Name of the column. */
      private String name;

      /**
       * Creates a new column enumeration entry with the given position, type, size and name.
       *
       * @param pos
       *           column position
       * @param type
       *           column type
       * @param size
       *           column size
       * @param name
       *           column name
       */
      Column(final int pos, final DataType type, final int size, final String name) {
         this.pos = pos;
         this.type = type;
         this.size = size;
         this.name = name;
      }

      /**
       * Creates a new column enumeration entry with the given position, type, size and name.
       *
       * @param pos
       *           column position
       * @param type
       *           column type
       * @param name
       *           column name
       */
      Column(final int pos, final DataType type, final String name) {
         this.pos = pos;
         this.type = type;
         this.size = type.getSize();
         this.name = name;
      }

      /**
       * Returns the position of this column.
       *
       * @return column position
       */
      public int getPosition() {
         return this.pos;
      }

      /**
       * Returns the type of this column.
       *
       * @return column type
       */
      public DataType getType() {
         return this.type;
      }

      /**
       * Returns the size of this column.
       *
       * @return column size
       */
      public int getSize() {
         return this.size;
      }

      /**
       * Returns the name of this column.
       *
       * @return column name
       */
      public String getName() {
         return this.name;
      }

      /**
       * Adds a field to the given schema builder that corresponds to this column.
       *
       * @param builder
       *           schema builder to populate
       */
      public void addColumn(final SchemaBuilder builder) {
         builder.addField(this.name, this.type, this.size);
      }
   }

   /** Schema of the tables table. */
   static final Schema TABLES_SCHEMA;

   static {
      // systables(tabid, name, order, card, idctr)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.TABLES_TABID.addColumn(builder);
      Column.TABLES_NAME.addColumn(builder);
      Column.TABLES_ORDER.addColumn(builder);
      Column.TABLES_CARD.addColumn(builder);
      Column.TABLES_IDCTR.addColumn(builder);
      TABLES_SCHEMA = builder.build();
   }

   /** Schema of the columns table. */
   static final Schema COLUMNS_SCHEMA;

   static {
      // syscolumns(colid, tabid, colno, name, type, size, card, ucard, min, max)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.COLUMNS_COLID.addColumn(builder);
      Column.COLUMNS_TABID.addColumn(builder);
      Column.COLUMNS_COLNO.addColumn(builder);
      Column.COLUMNS_NAME.addColumn(builder);
      Column.COLUMNS_TYPE.addColumn(builder);
      Column.COLUMNS_SIZE.addColumn(builder);
      Column.COLUMNS_CARD.addColumn(builder);
      Column.COLUMNS_UCARD.addColumn(builder);
      Column.COLUMNS_MIN.addColumn(builder);
      Column.COLUMNS_MAX.addColumn(builder);
      COLUMNS_SCHEMA = builder.build();
   }

   /** Schema of the constraints table. */
   static final Schema CONSTRAINTS_SCHEMA;

   static {
      // sysconstraints(constid, tabid, colid, name, type)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.CONSTRAINTS_CONSTID.addColumn(builder);
      Column.CONSTRAINTS_TABID.addColumn(builder);
      Column.CONSTRAINTS_COLID.addColumn(builder);
      Column.CONSTRAINTS_NAME.addColumn(builder);
      Column.CONSTRAINTS_TYPE.addColumn(builder);
      CONSTRAINTS_SCHEMA = builder.build();
   }

   /** Schema of the primary keys table. */
   static final Schema PRIMARYKEYS_SCHEMA;

   static {
      // sysprimarykeys(constid, tabid, colid, keyno)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.PRIMARYKEYS_CONSTID.addColumn(builder);
      Column.PRIMARYKEYS_TABID.addColumn(builder);
      Column.PRIMARYKEYS_COLID.addColumn(builder);
      Column.PRIMARYKEYS_KEYNO.addColumn(builder);
      PRIMARYKEYS_SCHEMA = builder.build();
   }

   /** Schema of the foreign keys table. */
   static final Schema FOREIGNKEYS_SCHEMA;

   static {
      // sysforeignkeys(constid, fkeyid, rkeyid, fkey, rkey, keyno)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.FOREIGNKEYS_CONSTID.addColumn(builder);
      Column.FOREIGNKEYS_FKEYID.addColumn(builder);
      Column.FOREIGNKEYS_RKEYID.addColumn(builder);
      Column.FOREIGNKEYS_FKEY.addColumn(builder);
      Column.FOREIGNKEYS_RKEY.addColumn(builder);
      Column.FOREIGNKEYS_KEYNO.addColumn(builder);
      FOREIGNKEYS_SCHEMA = builder.build();
   }

   /** Schema of the indexes table. */
   // TODO Cardinality and unique cardinality!
   static final Schema INDEXES_SCHEMA;

   static {
      // sysindexes(idxid, tabid, name, type, clustered)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.INDEXES_IDXID.addColumn(builder);
      Column.INDEXES_TABID.addColumn(builder);
      Column.INDEXES_NAME.addColumn(builder);
      Column.INDEXES_TYPE.addColumn(builder);
      Column.INDEXES_CLUSTERED.addColumn(builder);
      INDEXES_SCHEMA = builder.build();
   }

   /** Schema of the index keys table. */
   static final Schema INDEXKEYS_SCHEMA;

   static {
      // sysindexkeys(idxid, tabid, colid, order, keyno)
      final SchemaBuilder builder = new SchemaBuilder();
      Column.INDEXKEYS_IDXID.addColumn(builder);
      Column.INDEXKEYS_TABID.addColumn(builder);
      Column.INDEXKEYS_COLID.addColumn(builder);
      Column.INDEXKEYS_ORDER.addColumn(builder);
      Column.INDEXKEYS_KEYNO.addColumn(builder);
      INDEXKEYS_SCHEMA = builder.build();
   }

   /** List of all schemas defined by the system catalog. */
   static final Schema[] ALL_SCHEMAS = new Schema[] { TABLES_SCHEMA, COLUMNS_SCHEMA, CONSTRAINTS_SCHEMA,
         PRIMARYKEYS_SCHEMA, FOREIGNKEYS_SCHEMA, INDEXES_SCHEMA, INDEXKEYS_SCHEMA };

   /**
    * Hidden constructor.
    */
   private SystemCatalogSchema() {
      // hidden constructor
   }
}

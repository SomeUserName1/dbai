/*
 * @(#)SailorsCatalogUtil.java   1.0   Sep 8, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import minibase.storage.file.DiskManager;

/**
 * Utility that creates tables of the Sailors example database schema.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 */
public final class SailorsCatalogUtil {

   /** Cardinality of the "Sailors" table. */
   protected static final long SAILORS_CARD = 750;
   /** Cardinality of the "Boats" table. */
   protected static final long BOATS_CARD = 250;
   /** Cardinality of the "Reserves" table. */
   protected static final long RESERVES_CARD = 1500;

   /** Name of the "Sailors" table. */
   public static final String TBL_SAILORS = "Sailors";
   /** Name of the "Boats" table. */
   public static final String TBL_BOATS = "Boats";
   /** Name of the "Reserves" table. */
   public static final String TBL_RESERVES = "Reserves";
   /** Name of the "sid" attribute. */
   public static final String ATT_SID = "sid";
   /** Name of the "sname" attribute. */
   public static final String ATT_SNAME = "sname";
   /** Name of the "rating" attribute. */
   public static final String ATT_RATING = "rating";
   /** Name of the "age" attribute. */
   public static final String ATT_AGE = "age";
   /** Name of the "bid" attribute. */
   public static final String ATT_BID = "bid";
   /** Name of the "bname" attribute. */
   public static final String ATT_BNAME = "bname";
   /** Name of the "color" attribute. */
   public static final String ATT_COLOR = "color";
   /** Name of the "day" attribute. */
   public static final String ATT_DAY = "day";
   /** Name of the "rname" attribute. */
   public static final String ATT_RNAME = "rname";
   /** Name of the "pk_Reserves" primary key. */
   public static final String PK_RESERVES = "pk_reserves";
   /** Name of the index on Sailors.name. */
   public static final String IDX_SAILORS_NAME = "idx_sailors_name";
   /** Name of the index on Sailors.sid. */
   public static final String IDX_SAILORS_SID = "idx_sailors_sid";
   /** Name of the index on Sailors.rating. */
   public static final String IDX_SAILORS_RATING = "idx_sailors_rating";
   /** Name of the index on (Reserves.sid, Reserves.bid). */
   public static final String IDX_RESERVES_SIDBID = "idx_reserves_sidbid";

   /** Width of tuples of the Sailors table (4 + 25 + 4 + 4). */
   public static final double SAILORS_WIDTH = width(37);
   /** Width of tuples of the Boats table (4 + 25 + 10). */
   public static final double BOATS_WIDTH = width(39);
   /** Width of tuple of the Reserves table (4 + 4 + 8 + 25). */
   public static final double RESERVES_WIDTH = width(41);

   /**
    * Set up method that creates a simple system catalog.
    *
    * @param catalog
    *           system catalog
    */
   public static void initSystemCatalog(final SystemCatalog catalog) {

      // create Sailors table
      TableStatistics tableStatistics = new TableStatistics(SAILORS_CARD, 0, SAILORS_WIDTH);
      final int sailorsID = catalog.createTable(tableStatistics, TBL_SAILORS);

      ColumnStatistics columnStatistics = new ColumnStatistics(SAILORS_CARD, SAILORS_CARD, 0, SAILORS_CARD,
            width(4));
      final int sSid = catalog.createColumn(columnStatistics, ATT_SID, DataType.INT, 0, sailorsID);
      catalog.createColumnPrimaryKey(null, sailorsID, sSid);

      columnStatistics = new ColumnStatistics(SAILORS_CARD, (long) (SAILORS_CARD * 0.9), -1, -1, width(25));
      final int sName = catalog.createColumn(columnStatistics, ATT_SNAME, DataType.VARCHAR, 25, sailorsID);
      columnStatistics = new ColumnStatistics(SAILORS_CARD, 6, 0, 5, width(4));
      final int sRating = catalog.createColumn(columnStatistics, ATT_RATING, DataType.INT, 0, sailorsID);
      columnStatistics = new ColumnStatistics(SAILORS_CARD, 102, 18, 120, width(4));
      catalog.createColumn(columnStatistics, ATT_AGE, DataType.FLOAT, 0, sailorsID);

      // create index on Sailors
      IndexStatistics indexStatistics = new IndexStatistics(SAILORS_CARD, SAILORS_CARD);
      catalog.createIndex(indexStatistics, IDX_SAILORS_SID, IndexType.BTREE, true, new int[] { sSid },
            new SortOrder[] { SortOrder.ASCENDING }, sailorsID);
      indexStatistics = new IndexStatistics(SAILORS_CARD, (long) (SAILORS_CARD * 0.9));
      catalog.createIndex(indexStatistics, IDX_SAILORS_NAME, IndexType.SHASH, false, new int[] { sName },
            sailorsID);
      indexStatistics = new IndexStatistics(SAILORS_CARD, 6);
      catalog.createIndex(indexStatistics, IDX_SAILORS_RATING, IndexType.BTREE, false, new int[] { sRating },
            new SortOrder[] { SortOrder.DESCENDING }, sailorsID);

      // create Boats table
      tableStatistics = new TableStatistics(BOATS_CARD, 0, BOATS_WIDTH);
      final int boatsID = catalog.createTable(tableStatistics, TBL_BOATS);
      columnStatistics = new ColumnStatistics(BOATS_CARD, BOATS_CARD, 0, BOATS_CARD, width(4));
      final int bBid = catalog.createColumn(columnStatistics, ATT_BID, DataType.INT, 0, boatsID);
      catalog.createColumnPrimaryKey(null, boatsID, bBid);
      columnStatistics = new ColumnStatistics(BOATS_CARD, BOATS_CARD, -1, -1, width(25));
      catalog.createColumn(columnStatistics, ATT_BNAME, DataType.VARCHAR, 25, boatsID);
      columnStatistics = new ColumnStatistics(BOATS_CARD, 10, -1, -1, width(10));
      catalog.createColumn(columnStatistics, ATT_COLOR, DataType.VARCHAR, 10, boatsID);

      // create Reserves table
      tableStatistics = new TableStatistics(RESERVES_CARD, 0, RESERVES_WIDTH);
      final int reservesID = catalog.createTable(tableStatistics, TBL_RESERVES);
      columnStatistics = new ColumnStatistics(RESERVES_CARD, SAILORS_CARD, 0, SAILORS_CARD, width(4));
      final int rSid = catalog.createColumn(columnStatistics, ATT_SID, DataType.INT, 0, reservesID);
      catalog.createColumnForeignKey(null, reservesID, rSid, sailorsID, sSid);
      columnStatistics = new ColumnStatistics(RESERVES_CARD, BOATS_CARD, 0, BOATS_CARD, width(4));
      final int rBid = catalog.createColumn(columnStatistics, ATT_BID, DataType.INT, 0, reservesID);
      catalog.createColumnForeignKey(null, reservesID, rBid, boatsID, bBid);
      columnStatistics = new ColumnStatistics(RESERVES_CARD, (long) (RESERVES_CARD * 0.75), -1, -1, width(8));
      final int rDay = catalog.createColumn(columnStatistics, ATT_DAY, DataType.DATETIME, 0, reservesID);
      columnStatistics = new ColumnStatistics(RESERVES_CARD, (long) (RESERVES_CARD * 0.9), -1, -1, width(25));
      catalog.createColumn(columnStatistics, ATT_RNAME, DataType.VARCHAR, 25, reservesID);
      catalog.createTablePrimaryKey(PK_RESERVES, reservesID, new int[] { rSid, rBid, rDay });

      // create index on Reserves
      indexStatistics = new IndexStatistics(RESERVES_CARD, (long) (RESERVES_CARD * 0.8));
      catalog.createIndex(indexStatistics, IDX_RESERVES_SIDBID, IndexType.BTREE, false,
            new int[] { rSid, rBid }, new SortOrder[] { SortOrder.ASCENDING, SortOrder.ASCENDING },
            reservesID);
   }

   /**
    * Returns the width as a fraction of the page size based on the given size in bytes.
    *
    * @param size
    *           size in bytes
    * @return width as a fraction of the page size
    */
   private static double width(final int size) {
      return (double) size / DiskManager.PAGE_SIZE;
   }

   /**
    * Hidden constructor.
    */
   private SailorsCatalogUtil() {
      // hidden constructor
   }
}

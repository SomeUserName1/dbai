/*
 * @(#)XmlToSystemCatalog.java   1.0   Jun 30, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.util;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import minibase.catalog.ColumnStatistics;
import minibase.catalog.DataType;
import minibase.catalog.IndexStatistics;
import minibase.catalog.IndexType;
import minibase.catalog.SortOrder;
import minibase.catalog.SystemCatalog;
import minibase.catalog.TableDescriptor;
import minibase.catalog.TableStatistics;
import minibase.catalog.v1.Column;
import minibase.catalog.v1.ForeignKey;
import minibase.catalog.v1.Index;
import minibase.catalog.v1.Table;

/**
 * Clears the given system catalog and creates new tables/columns/indexes structure according to a xml
 * formated definition. Namespace:"http://minibase/catalog/v1". UTF-8 encoding is expected.
 *
 * @author Marcel Hanser, &lt;marcel.hanser@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class XmlToSystemCatalog {

   /**
    * Utility class.
    */
   private XmlToSystemCatalog() {
   }

   /**
    * Clears the given system catalog and fills it with the XML formated data in the input stream. UTF-8
    * encoding is expected.
    *
    * @param catalog
    *           to fill
    * @param stream
    *           XML stream
    * @throws JAXBException
    *            if there is a parsing exception
    */
   public static void loadFrom(final SystemCatalog catalog, final InputStream stream) throws JAXBException {
      final minibase.catalog.v1.SystemCatalog xmlSys = (minibase.catalog.v1.SystemCatalog) JAXBContext
            .newInstance("minibase.catalog.v1").createUnmarshaller().unmarshal(stream);

      if (catalog.getPageSize() != xmlSys.getPageSize()) {
         throw new IllegalStateException(
               "The page size of the given system catalog does not match the one of the XML file: "
                     + catalog.getPageSize() + " != " + xmlSys.getPageSize() + ".");
      }

      final Map<TableDescriptor, Collection<ForeignKey>> foreignKeys = new HashMap<>();

      // add all tables, their columns and indexes, the foreign keys are done later on.
      for (final Table table : xmlSys.getTables().getTable()) {

         final TableStatistics tableStatistics = new TableStatistics(table.getCardinality(),
               table.getCardinality(), table.getWidth() / xmlSys.getPageSize());
         final int tableId = catalog.createTable(tableStatistics, table.getName());

         final Map<String, Integer> colMap = new HashMap<>();

         for (final Column col : table.getColumns().getColumn()) {

            final int numDistinctVals = col.getDistinctValues() == null ? -1 : col.getDistinctValues();

            final DataType dataType = getDataType(col.getType());

            final Object maximum = col.getMaximum() == null ? null : dataType.parse(col.getMaximum());
            final Object minimum = col.getMinimum() == null ? null : dataType.parse(col.getMinimum());

            final ColumnStatistics columnStatistics = new ColumnStatistics(table.getCardinality(),
                  numDistinctVals, minimum, maximum, col.getWidth() / xmlSys.getPageSize());
            final int colId = catalog.createColumn(columnStatistics, col.getName(),
                  getDataType(col.getType()), (int) col.getWidth(), tableId);
            colMap.put(col.getName(), colId);
            colMap.put(table.getName() + "." + col.getName(), colId);
         }

         if (table.getIndexes() != null) {
            for (final Index index : table.getIndexes().getIndex()) {

               final IndexStatistics indexStatistics = new IndexStatistics(index.getCardinality(),
                     index.getCardinality());
               final int[] colIds = index.getKeyColumns().getColumn().stream()
                     .mapToInt(col -> assertColumnID(table, colMap, col.getRef())).toArray();
               // TODO Read key column sort order from XML
               final SortOrder[] colOrders = new SortOrder[colIds.length];
               Arrays.fill(colOrders, SortOrder.ASCENDING);
               catalog.createIndex(indexStatistics, index.getName(), getIndexType(index.getType()),
                     index.isClustered(), colIds, colOrders, tableId);
            }
         }

         if (table.getPrimaryKey() != null) {
            catalog.createTablePrimaryKey("prim_" + table.getName(), tableId,
                  table.getPrimaryKey().getColumns().getColumn().stream()
                        .mapToInt(col -> assertColumnID(table, colMap, col.getRef())).toArray());
         }
         foreignKeys.put(catalog.getTableDescriptor(tableId), table.getForeignKey());
      }

      for (final Map.Entry<TableDescriptor, Collection<ForeignKey>> forKeyEntry : foreignKeys.entrySet()) {
         int i = 0;
         final TableDescriptor tableDisc = forKeyEntry.getKey();
         for (final ForeignKey foreignKey : forKeyEntry.getValue()) {
            final int[] sourceCols = findColumns(tableDisc, foreignKey.getSourceColumns().getColumn().stream()
                  .map(col -> col.getRef()).collect(Collectors.toList()));
            final int[] targetCols = findColumns(catalog, foreignKey.getTargetColumns().getColumn().stream()
                  .map(col -> col.getTable() + "." + col.getRef()).collect(Collectors.toList()));
            final TableDescriptor refTable = catalog
                  .getTable(foreignKey.getTargetColumns().getColumn().get(0).getTable());
            catalog.createTableForeignKey(tableDisc.getName() + "_foreignKey_" + i++,
                  tableDisc.getCatalogID(), sourceCols, refTable.getCatalogID(), targetCols);
         }
      }
   }

   /**
    * Parses the given column names in the {@code table.column} format and returns their corresponding catalog
    * identifiers.
    *
    * @param systemCatalog
    *           system catalog
    * @param columns
    *           column names
    * @return column identifiers
    */
   private static int[] findColumns(final SystemCatalog systemCatalog, final List<String> columns) {
      if (columns.isEmpty()) {
         return new int[0];
      } else {
         final Set<TableDescriptor> targetTables = new HashSet<>();
         for (final String col : columns) {
            final String string = col;
            final List<String> splitToList = Arrays.asList(string.split("\\."));
            if (splitToList.size() != 2) {
               System.out.println(splitToList);
               throw new IllegalArgumentException(
                     "Column definition not complete - (should be:{table}.{col}): '" + string + "'.");
            }
            targetTables.add(systemCatalog.getTable(splitToList.get(0)));
            if (targetTables.size() > 1) {
               throw new IllegalArgumentException("Foreign Key constraint targets multiple tables: '"
                     + targetTables.stream().map(td -> td.getName()).toArray() + "'.");
            }
         }
         final TableDescriptor tableDescriptor = targetTables.iterator().next();
         return findColumns(tableDescriptor, columns);
      }
   }

   /**
    * Parses the given column names in the {@code table.column} format and returns their corresponding catalog
    * identifiers.
    *
    * @param tableDesc
    *           table descriptor
    * @param columns
    *           column names
    * @return column identifiers
    */
   private static int[] findColumns(final TableDescriptor tableDesc, final List<String> columns) {
      return columns.stream().mapToInt(s -> assertColumnID(tableDesc, s)).toArray();
   }

   /**
    * Asserts that a column with the given name exists in the given table and returns its system catalog
    * identifier from the given column map.
    *
    * @param table
    *           table
    * @param columns
    *           column map
    * @param column
    *           column name
    * @return column identifier
    */
   private static Integer assertColumnID(final Table table, final Map<String, Integer> columns,
         final String column) {
      return Objects.requireNonNull(columns.get(column),
            String.format("Column: '%s' not registered on Table: '%s'", column, table.getName()));
   }

   /**
    * Asserts that a column with the given name is defined for the given table descriptor and returns its
    * system catalog identifier.
    *
    * @param tableDesc
    *           table descriptor
    * @param column
    *           column name
    * @return column identifier
    */
   private static int assertColumnID(final TableDescriptor tableDesc, final String column) {
      String clean = column;
      if (clean.contains(".")) {
         final List<String> splitToList = Arrays.asList(clean.split("\\."));
         if (splitToList.size() != 2) {
            throw new IllegalArgumentException(
                  "Column definition not complete - (should be:{table}.{col}): '" + clean + "'.");
         }
         clean = splitToList.get(1);
      }
      return Objects
            .requireNonNull(tableDesc.getColumn(clean),
                  String.format("Column: '%s' not registered on Table: '%s'", clean, tableDesc.getName()))
            .getCatalogID();
   }

   /**
    * Parses the given type name and returns the corresponding Minibase data type.
    *
    * @param toParse
    *           type name
    * @return data type
    */
   private static DataType getDataType(final String toParse) {
      final int optIndex = toParse.indexOf('(');
      final String typeName = optIndex < 0 ? toParse : toParse.substring(0, optIndex);
      switch (typeName.toLowerCase()) {
         case "character varying":
            return DataType.VARCHAR;
         case "numeric":
            return DataType.DOUBLE;
         case "integer":
            return DataType.INT;
         case "date":
            return DataType.DATE;
         case "character":
            return DataType.CHAR;
         default:
            throw new IllegalArgumentException(String.format("DataType: %s not supported.", toParse));
      }
   }

   /**
    * Returns the index type that corresponds to the given type string.
    *
    * @param type
    *           type string
    * @return index type
    */
   private static IndexType getIndexType(final String type) {
      return IndexType.valueOf(type.toUpperCase());
   }
}

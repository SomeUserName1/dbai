/*
 * @(#)TableScan.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import minibase.access.file.File;
import minibase.access.file.FileScan;
import minibase.query.schema.Schema;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator" version takes schema into
 * consideration and generates real tuples.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public final class TableScan extends AbstractOperator {

   /** The heap file to scan. */
   private final File file;

   /**
    * Constructs a file scan, given the schema and heap file.
    *
    * @param schema
    *           the schema of the returned tuples
    * @param file
    *           the heap file to scan
    */
   public TableScan(final Schema schema, final File file) {
      super(schema);
      this.file = file;
   }

   @Override
   public FileScan open() {
      return this.file.openScan();
   }
}

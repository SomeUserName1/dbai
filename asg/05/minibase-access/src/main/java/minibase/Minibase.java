/*
 * @(#)Minibase.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import java.io.Closeable;
import java.io.IOException;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.ReplacementStrategy;
import minibase.storage.file.DiskManager;

/**
 * Definitions for the running Minibase system, including references to static layers and database-level
 * attributes.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public class Minibase implements Closeable {

   /** Name of the data file. */
   private final String databaseName;

   /** The Minibase Buffer Manager. */
   private final BufferManager bufferManager;

   // --------------------------------------------------------------------------

   /**
    * Constructs and starts an instance of Minibase, given the configuration.
    *
    * @param dbname
    *           Name of the data file
    * @param numPages
    *           Number of pages to allocate
    * @param bufferPoolSize
    *           Buffer pool size (in pages)
    * @param replacementPolicy
    *           Buffer pool replacement policy
    * @param exists
    *           If the database already exists on disk
    */
   public Minibase(final String dbname, final int numPages, final int bufferPoolSize,
         final ReplacementStrategy replacementPolicy, final boolean exists) {
      // save the file name
      this.databaseName = dbname;

      // load the static layers
      try {
         final DiskManager diskManager = exists ? new DiskManager(dbname) : new DiskManager(dbname, numPages);
         this.bufferManager = new BufferManager(diskManager, bufferPoolSize, replacementPolicy);
      } catch (final Exception exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Returns the name of the database.
    *
    * @return The database name.
    */
   public String getDatabaseName() {
      return this.databaseName;
   }

   /**
    * Returns a reference to the buffer Manager.
    *
    * @return The buffer manager.
    */
   public BufferManager getBufferManager() {
      return this.bufferManager;
   }

   /**
    * Flushes all cached data to disk.
    */
   public void flush() {
      this.bufferManager.flushAllPages();
      this.bufferManager.getDiskManager().flushAllPages();
   }

   @Override
   public void close() throws IOException {
      this.flush();
      this.bufferManager.getDiskManager().close();
   }

   /**
    * Displays an unrecoverable error and halts the system.
    *
    * @param exc
    *           the exception triggering the shutdown
    * @return never
    */
   public static Error haltSystem(final Exception exc) {
      System.err.println("\n*** Unrecoverable system error ***");
      exc.printStackTrace();
      Runtime.getRuntime().exit(1);
      // will never happen
      return new Error();
   }
}

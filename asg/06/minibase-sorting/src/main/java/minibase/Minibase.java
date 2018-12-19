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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.ReplacementStrategy;
import minibase.storage.file.DiskFile;
import minibase.storage.file.DiskManager;

/**
 * Definitions for the running Minibase system, including references to static layers and database-level
 * attributes.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @version 1.0
 */
public final class Minibase implements Closeable {

   /** Name of the data file. */
   private final File databaseFile;

   /** The Minibase Buffer Manager. */
   private final BufferManager bufferManager;

   /**
    * Creates a new Minibase instance.
    *
    * @param databaseFile database file
    * @param bufferManager buffer manager
    */
   private Minibase(final File databaseFile, final BufferManager bufferManager) {
      this.databaseFile = databaseFile;
      this.bufferManager = bufferManager;
   }

   /**
    * Creates a new Minibase database.
    *
    * @param dbFile
    *           name of the data file
    * @param numPages
    *           number of pages to allocate
    * @param bufferPoolSize
    *           buffer pool size (in pages)
    * @param replacementPolicy
    *           buffer pool replacement policy
    * @return minibase instance
    */
   public static Minibase create(final String dbFile, final int numPages, final int bufferPoolSize,
         final ReplacementStrategy replacementPolicy) {
      try {
         // delete any old database with the same name
         final File file = new File(dbFile);
         if (file.exists()) {
            Files.delete(file.toPath());
         }

         // load the static layers
         final DiskManager diskManager = DiskManager.create(DiskFile.create(file, numPages));
         final BufferManager bufferManager =
               new BufferManager(diskManager, bufferPoolSize, replacementPolicy);
         return new Minibase(diskManager.getDatabaseFile(), bufferManager);
      } catch (final Exception exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Creates a new temporary Minibase database.
    *
    * @param prefix
    *           name prefix for the temporary file
    * @param numPages
    *           number of pages to allocate
    * @param bufferPoolSize
    *           buffer pool size (in pages)
    * @param replacementPolicy
    *           buffer pool replacement policy
    * @return minibase instance
    */
   public static Minibase createTemporary(final String prefix, final int numPages, final int bufferPoolSize,
         final ReplacementStrategy replacementPolicy) {
      try {
         // load the static layers
         final File temp = File.createTempFile("temp_" + prefix, ".minibase");
         temp.deleteOnExit();
         final DiskManager diskManager = DiskManager.create(DiskFile.create(temp, numPages));
         final BufferManager bufferManager =
               new BufferManager(diskManager, bufferPoolSize, replacementPolicy);
         return new Minibase(diskManager.getDatabaseFile(), bufferManager);
      } catch (final Exception exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Opens an existing Minibase database.
    *
    * @param dbFile
    *           name of the data file
    * @param bufferPoolSize
    *           buffer pool size (in pages)
    * @param replacementPolicy
    *           buffer pool replacement policy
    * @return minibase instance
    */
   public static Minibase open(final String dbFile, final int bufferPoolSize,
         final ReplacementStrategy replacementPolicy) {
      try {
         // load the static layers
         final DiskFile diskFile = DiskFile.open(new File(dbFile));
         final DiskManager diskManager = DiskManager.open(diskFile);
         final BufferManager bufferManager =
               new BufferManager(diskManager, bufferPoolSize, replacementPolicy);
         return new Minibase(diskManager.getDatabaseFile(), bufferManager);
      } catch (final Exception exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Returns the name of the database.
    *
    * @return The database name.
    */
   public File getDatabaseFile() {
      return this.databaseFile;
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
    * Closes the database and deletes the database file.
    *
    * @throws IOException if the database cannot be deleted
    */
   public void delete() throws IOException {
      this.flush();
      this.bufferManager.getDiskManager().destroy();
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

/*
 * @(#)DiskFile.java   1.0   Oct 29, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.storage.file;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import minibase.Minibase;

/**
 * A database file on disk.
 *
 * @author Leo Woerteler &t;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class DiskFile implements Closeable {

   /** The file name. */
   private final File fileName;

   /** The opened database file. */
   private final RandomAccessFile file;

   /** Number of pages this disk file contains. */
   private final long numPages;

   /**
    * Creates a new disk file instance.
    *
    * @param fileName name of the disk file
    * @param file random-access file
    * @param numPages number of database pages
    */
   private DiskFile(final File fileName, final RandomAccessFile file, final long numPages) {
      this.fileName = fileName;
      this.file = file;
      this.numPages = numPages;
   }

   /**
    * Creates a new database file with the given name and number of pages.
    *
    * @param fileName
    *           name o the database file
    * @param numPages
    *           number of pages in the file
    * @return disk file instance
    */
   public static DiskFile create(final File fileName, final long numPages) {
      if (numPages < 2) {
         throw new IllegalArgumentException("Database size too small: " + numPages);
      }
      try {
         final RandomAccessFile file = new RandomAccessFile(fileName, "rw");
         file.setLength(numPages * DiskManager.PAGE_SIZE);
         return new DiskFile(fileName, file, numPages);
      } catch (final IOException exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Opens an existing disk file.
    *
    * @param fileName
    *           name of the file
    * @return disk file instance
    */
   public static DiskFile open(final File fileName) {
      try {
         final RandomAccessFile file = new RandomAccessFile(fileName, "rw");
         final long size = file.length();
         if (size % DiskManager.PAGE_SIZE != 0) {
            file.close();
            throw new IOException("Wrong size for database file: " + size);
         }
         final long numPages = size / DiskManager.PAGE_SIZE;
         if (numPages < 2) {
            file.close();
            throw new IOException("Database file too small: " + size);
         }
         return new DiskFile(fileName, file, numPages);
      } catch (final IOException exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Getter for this file's name.
    *
    * @return the file name
    */
   public File getFileName() {
      return this.fileName;
   }

   /**
    * Number of pages in this disk file.
    *
    * @return number of pages
    */
   public long getNumPages() {
      return this.numPages;
   }

   /**
    * Reads the contents of the specified page from disk.
    *
    * @param pageNo
    *           identifies the page to read
    * @param data
    *           output parameter to hold the contents of the page
    * @throws IllegalArgumentException
    *            if pageID is invalid
    */
   public void readPage(final int pageNo, final byte[] data) {
      // seek to the correct page on disk and read it
      try {
         this.file.seek(1L * pageNo * DiskManager.PAGE_SIZE);
         this.file.readFully(data);
      } catch (final IOException exc) {
         Minibase.haltSystem(exc);
      }
   }

   /**
    * Writes the contents of the given page to disk.
    *
    * @param pageNo
    *           identifies the page to write
    * @param data
    *           holds the contents of the page
    */
   public void writePage(final int pageNo, final byte[] data) {
      // seek to the correct page on disk and write it
      try {
         this.file.seek(1L * pageNo * DiskManager.PAGE_SIZE);
         this.file.write(data);
      } catch (final IOException exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   @Override
   public void close() throws IOException {
      this.file.close();
   }
}

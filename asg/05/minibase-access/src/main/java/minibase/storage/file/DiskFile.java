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
class DiskFile implements Closeable {

   /** The file name. */
   private final File fileName;

   /** The opened database file. */
   private final RandomAccessFile file;

   /**
    * Constructor creating a new database file with the given name and number of pages.
    *
    * @param fileName
    *           name o the database file
    * @param numPages
    *           number of pages in the file
    */
   DiskFile(final File fileName, final int numPages) {
      this(fileName);
      try {
         this.file.seek(numPages * DiskManager.PAGE_SIZE - 1);
         this.file.writeByte(0);
      } catch (final IOException exc) {
         throw Minibase.haltSystem(exc);
      }
   }

   /**
    * Constructor opening an existing disk file.
    *
    * @param fileName
    *           name of the file
    */
   DiskFile(final File fileName) {
      this.fileName = fileName;
      try {
         this.file = new RandomAccessFile(fileName, "rw");
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
         this.file.seek(pageNo * DiskManager.PAGE_SIZE);
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
         this.file.seek(pageNo * DiskManager.PAGE_SIZE);
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

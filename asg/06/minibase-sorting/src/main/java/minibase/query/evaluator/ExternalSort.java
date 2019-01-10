/*
 * @(#)ExternalSort.java   1.0   Dec 18, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.ArrayList;

import minibase.access.file.ReplacementSortBuffer;
import minibase.access.file.Run;
import minibase.access.file.RunBuilder;
import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.schema.Schema;
import minibase.storage.buffer.BufferManager;

/**
 * Implementing Merge Sort via tree of losers.
 *
 * @author Fabian Klopfer, Simon Suckut
 */
public class ExternalSort extends AbstractOperator {
   /** The schema the data is described by. */
   private final Schema schema;

   /** The buffer manager. */
   private final BufferManager bufferManager;

   /** Operator the tuples come from. */
   private final Operator input;

   /** The comparator the tuples can be divided by. */
   private final RecordComparator comparator;

   /** How many pages is the replacement sort allowed to use. */
   private final int bufferSize;

   /**
    * Constructor.
    * 
    * @param schema
    *           Schema of the internal tuples
    * @param bufferManager
    *           where the pages come from
    * @param input
    *           operator giving the input data for the sort
    * @param comparator
    *           Compares the tuples with each other
    * @param bufferSize
    *           How many pages can be used for the currentSet in Replacement Sort.
    */
   ExternalSort(final Schema schema, final BufferManager bufferManager, final Operator input,
         final RecordComparator comparator, final int bufferSize) {
      super(schema);
      this.schema = schema;
      this.bufferManager = bufferManager;
      this.input = input;
      this.comparator = comparator;
      this.bufferSize = bufferSize;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public TupleIterator open() {
      // Iterator to get input values from
      final TupleIterator inputIterator = this.input.open();
      // List of all runs which are then given to the tuple iterator at the and
      final ArrayList<Run> runs0 = new ArrayList<Run>();
      // Reused to generate the runs with
      RunBuilder runBuilder = new RunBuilder(this.bufferManager, this.schema.getLength());
      
      // Replacement Buffer to get records from.
      final ReplacementSortBuffer currentSet = new ReplacementSortBuffer(this.bufferManager, this.bufferSize,
            this.schema.getLength(), this.comparator, inputIterator);

      while (currentSet.hasNext()) {
         final byte[] replacement = currentSet.next();

         if (replacement != null) {
            runBuilder.appendRecord(replacement);
         } else {
            runs0.add(runBuilder.finish());
            runBuilder = new RunBuilder(this.bufferManager, this.schema.getLength());
            currentSet.newRun();
         }
      }      
      runs0.add(runBuilder.finish());
      
      currentSet.close();
//      inputIterator.close();

      try {
         inputIterator.close();
      } catch (final UnsupportedOperationException e) {
      }
      // Return an TupleIterator representing a Tree of Losers.
      return new TreeOfLosers(runs0, this.comparator, this.schema.getLength(), this.bufferManager);
   }
}

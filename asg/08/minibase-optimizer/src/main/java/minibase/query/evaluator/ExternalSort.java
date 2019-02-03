/*
 * @(#)ExternalSort.java   1.0   Jan 11, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import minibase.access.file.Run;
import minibase.access.file.RunBuilder;
import minibase.access.file.RunPage;
import minibase.access.file.RunScan;
import minibase.catalog.DataType;
import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.schema.Schema;
import minibase.query.schema.SchemaBuilder;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;
import minibase.storage.file.DiskManager;

/**
 * A sort operator that sorts its input according to a given {@link RecordComparator}.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public class ExternalSort extends AbstractOperator {

   /** Schema of the internally used runs of intermediate runs. */
   static final Schema RUN_OF_RUNS =
         new SchemaBuilder()
               .addField("pageID", DataType.INT, PageID.BYTES)
               .addField("size", DataType.BIGINT, Long.BYTES).build();

   /** Buffer manager. */
   private final BufferManager bufferManager;

   /** Input relation to sort. */
   private final Operator input;

   /** Comparator that dictates the sorting order. */
   private final RecordComparator comparator;

   /** Number of pages used to to generate longer initial runs. values smaller than one mean single-page sorting. */
   private final int bufferPages;

   /** Maximum number of runs to merge at once. */
   private final int k;

   /**
    * Creates a sort operator that sorts the given input operator's result according to the given comparator.
    * It creates single-page initial runs using <i>insertion sort</i> and simple binary merge sort.
    *
    * @param bufferManager buffer manager
    * @param input input operator
    * @param comparator record comparator
    */
   public ExternalSort(final BufferManager bufferManager, final Operator input, final RecordComparator comparator) {
      this(bufferManager, input, comparator, 2, 0);
   }

   /**
    * Creates a sort operator that sorts the given input operator's result according to the given comparator.
    *
    * @param bufferManager buffer manager
    * @param input input operator
    * @param comparator record comparator
    * @param bufferPages number of buffer pages used for creating initial runs, values smaller than one
    *          lead to single-page initial runs built using <i>insertion sort</i>
    * @param k maximum number of runs that are merged simultaneously, values smaller than three lead to
    *          simple binary merge sort
    */
   public ExternalSort(final BufferManager bufferManager, final Operator input,
         final RecordComparator comparator, final int bufferPages, final int k) {
      super(input.getSchema());
      this.bufferManager = bufferManager;
      this.input = input;
      this.comparator = comparator;
      this.bufferPages = bufferPages;
      this.k = k;
   }

   /**
    * Reads the input relation and generates a run of initial sorted runs.
    *
    * @param iter iterator over the input relation
    * @return run of sorted initial runs
    */
   Run initialRuns(final TupleIterator iter) {
      final int recordLength = this.getSchema().getLength();
      if (this.bufferPages >= 1) {
         // Advanced: use replacement sort
         final int capacity = this.bufferPages * DiskManager.PAGE_SIZE / recordLength;
         return new ReplacementSorter(this.bufferManager, capacity, this.comparator, recordLength).initialRuns(iter);
      }

      // Basic: use single-page insertion sort
      final int capacity = RunPage.capacity(recordLength);
      final RunBuilder builder = new RunBuilder(this.bufferManager, RUN_OF_RUNS.getLength());
      final byte[] outerTuple = RUN_OF_RUNS.newTuple();
      do {
         final Page<RunPage> page = RunPage.initialize(this.bufferManager.newPage());
         final byte[] data = page.getData();
         int n = 0;
         while (n < capacity && iter.hasNext()) {
            final byte[] next = iter.next();
            int i = n;
            while (--i >= 0) {
               if (this.comparator.compare(data, i * recordLength, next, 0) <= 0) {
                  break;
               }
            }
            final int move = n - i - 1;
            if (move > 0) {
               System.arraycopy(data, (i + 1) * recordLength, data, (i + 2) * recordLength, move * recordLength);
            }
            System.arraycopy(next, 0, data, (i + 1) * recordLength, recordLength);
            n++;
         }
         RUN_OF_RUNS.setIntField(outerTuple, 0, page.getPageID().getValue());
         RUN_OF_RUNS.setBigintField(outerTuple, 1, n);
         this.bufferManager.unpinPage(page, UnpinMode.DIRTY);
         builder.appendRecord(outerTuple);
      } while (iter.hasNext());
      return builder.finish();
   }

   /**
    * Takes a run of initial runs and iteratively merges them together until a single sorted run remains.
    *
    * @param runOfRuns run of initial runs
    * @return single sorted run
    */
   Run mergePhase(final Run runOfRuns) {
      return this.k < 3 ? this.mergePhaseBasic(runOfRuns) : this.mergePhaseAdvanced(runOfRuns);
   }

   /**
    * Takes a run of initial runs and iteratively merges them together until a single sorted run remains.
    * This variant always merges two runs into one.
    *
    * @param runOfRuns run of initial runs
    * @return single sorted run
    */
   Run mergePhaseBasic(final Run runOfRuns) {
      final byte[] outerTuple = RUN_OF_RUNS.newTuple();
      Run current = runOfRuns;
      while (current.getLength() > 1) {
         final RunBuilder builder = new RunBuilder(this.bufferManager, RUN_OF_RUNS.getLength());
         try (RunScan scan = new RunScan(this.bufferManager, current, RUN_OF_RUNS.getLength())) {
            do {
               Run run = readRun(scan.next());
               if (scan.hasNext()) {
                   run = this.mergeRunsBasic(run, readRun(scan.next()));
               }
               RUN_OF_RUNS.setIntField(outerTuple, 0, run.getFirstPageID().getValue());
               RUN_OF_RUNS.setBigintField(outerTuple, 1, run.getLength());
               builder.appendRecord(outerTuple);
            } while (scan.hasNext());
         }
         current = builder.finish();
      }

      try (RunScan scan = new RunScan(this.bufferManager, current, RUN_OF_RUNS.getLength())) {
         return readRun(scan.next());
      }
   }

   /**
    * Merges two sorted runs into one.
    *
    * @param run1 first run to merge
    * @param run2 second run to merge
    * @return merged run
    */
   Run mergeRunsBasic(final Run run1, final Run run2) {
      final int recordLength = this.getSchema().getLength();
      final RunBuilder builder = new RunBuilder(this.bufferManager, recordLength);
      try (PeekableIterator iter1 = new PeekableIterator(new RunScan(this.bufferManager, run1, recordLength));
            PeekableIterator iter2 = new PeekableIterator(new RunScan(this.bufferManager, run2, recordLength))) {
         while (iter1.hasNext() && iter2.hasNext()) {
            builder.appendRecord(
                  this.comparator.compare(iter1.peek(), iter2.peek()) <= 0 ? iter1.next() : iter2.next());
         }
         while (iter1.hasNext()) {
            builder.appendRecord(iter1.next());
         }
         while (iter2.hasNext()) {
            builder.appendRecord(iter2.next());
         }
      }
      return builder.finish();
   }

   /**
    * Takes a run of initial runs and iteratively merges them together until a single sorted run remains.
    * This variant uses a <i>Tree of Losers</i> to merge more than two input runs at once.
    *
    * @param runOfRuns run of initial runs
    * @return single sorted run
    */
   Run mergePhaseAdvanced(final Run runOfRuns) {
      final byte[] outerTuple = RUN_OF_RUNS.newTuple();
      Run current = runOfRuns;
      final List<Run> runs = new ArrayList<>();
      while (current.getLength() > 1) {
         final RunBuilder builder = new RunBuilder(this.bufferManager, RUN_OF_RUNS.getLength());
         try (RunScan scan = new RunScan(this.bufferManager, current, RUN_OF_RUNS.getLength())) {
            do {
               for (int i = 0; i < this.k && scan.hasNext(); i++) {
                  runs.add(readRun(scan.next()));
               }
               final Run run = this.mergeRunsAdvanced(runs);
               runs.clear();
               RUN_OF_RUNS.setIntField(outerTuple, 0, run.getFirstPageID().getValue());
               RUN_OF_RUNS.setBigintField(outerTuple, 1, run.getLength());
               builder.appendRecord(outerTuple);
            } while (scan.hasNext());
         }
         current = builder.finish();
      }

      try (RunScan scan = new RunScan(this.bufferManager, current, RUN_OF_RUNS.getLength())) {
         return readRun(scan.next());
      }
   }

   /**
    * Merges the given runs into one using a <i>Tree of Losers</i>.
    * @param runs list of runs to merge
    * @return merged run
    */
   Run mergeRunsAdvanced(final List<Run> runs) {
      if (runs.size() == 1) {
         return runs.get(0);
      }

      final int recordLength = this.getSchema().getLength();
      final RunBuilder builder = new RunBuilder(this.bufferManager, recordLength);
      try (TupleIterator iter = new TreeOfLosers(this.comparator,
            runs.stream().map(r -> new RunScan(this.bufferManager, r, recordLength))
                  .toArray(n -> new TupleIterator[n]))) {
         while (iter.hasNext()) {
            builder.appendRecord(iter.next());
         }
      }
      return builder.finish();
   }

   /**
    * Reads a {@link Run} from a record.
    *
    * @param record record to read the run's data from
    * @return {@link Run} instance
    */
   private static Run readRun(final byte[] record) {
      final PageID id = PageID.getInstance(RUN_OF_RUNS.getIntField(record, 0));
      return new Run(id, RUN_OF_RUNS.getBigintField(record, 1));
   }

   @Override
   public TupleIterator open() {
      final Run runOfRuns;
      try (TupleIterator inputIter = this.input.open()) {
         if (!inputIter.hasNext()) {
            return TupleIterator.EMPTY;
         }
         runOfRuns = this.initialRuns(inputIter);
      }
      final Run finalRun = this.mergePhaseAdvanced(runOfRuns);
      return new RunScan(this.bufferManager, finalRun, this.getSchema().getLength());
   }
}

/**
 * Selection tree for merging more than two sorted input iterators.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
final class TreeOfLosers implements TupleIterator {

   /** Marker object for free slots. */
   private static final byte[] FREE_SLOT = new byte[0];

   /** Input iterators. */
   private final TupleIterator[] inputs;

   /** Comparator. */
   private final RecordComparator comparator;

   /** Array that contains the tree of losers. */
   private final byte[][] tree;

   /** Indexes of the input iterators that each element came from. */
   private final int[] origins;

   /**
    * Constructs a selection tree.
    *
    * @param comparator record comparator
    * @param inputs input iterators
    */
   TreeOfLosers(final RecordComparator comparator, final TupleIterator[] inputs) {
      this.comparator = comparator;
      this.inputs = inputs;
      this.origins = new int[inputs.length];
      this.tree = new byte[inputs.length][];
      this.fill();
   }

   /**
    * Fills the tree from the leaves.
    */
   private void fill() {
      Arrays.fill(this.tree, FREE_SLOT);
      Arrays.fill(this.origins, -1);
      final int n = this.inputs.length;
      for (int i = 0; i < n; i++) {
         byte[] value = this.inputs[i].hasNext() ? this.inputs[i].next() : null;
         int pos = (i + n) / 2;
         int origin = i;
         while (pos > 0 && this.tree[pos] != FREE_SLOT) {
            final byte[] other = this.tree[pos];
            final int otherOrigin = this.origins[pos];
            if (this.compare(origin, value, otherOrigin, other) > 0) {
               this.tree[pos] = value;
               this.origins[pos] = origin;
               value = other;
               origin = otherOrigin;
            }
            pos /= 2;
         }
         this.tree[pos] = value;
         this.origins[pos] = origin;
      }
   }

   /**
    * Compares two records that may be {@code null} (meaning greater than any existing record).
    * If two records compare as equal, they are compared by the index of their inputs to guarantee stable sorting.
    *
    * @param i input of the first record
    * @param a first record
    * @param j input of the second record
    * @param b second record
    * @return A value {@code < 0}, {@code == 0}, or {@code > 0}
    *          if {@code a} is smaller than, equal to or greater than {@code b}
    */
   private int compare(final int i, final byte[] a, final int j, final byte[] b) {
      if (a == null) {
         return b == null ? 0 : 1;
      }
      if (b == null) {
         return -1;
      }
      final int res = this.comparator.compare(a, b);
      return res == 0 ? Integer.compare(i, j) : res;
   }

   @Override
   public boolean hasNext() {
      return this.tree[0] != null;
   }

   @Override
   public byte[] next() {
      final byte[] next = this.tree[0];
      if (next == null) {
         throw new NoSuchElementException();
      }
      this.tree[0] = FREE_SLOT;
      this.refill(this.origins[0]);
      return next;
   }

   /**
    * Refills the tree with the next value from the input with the given index.
    *
    * @param input index of the last winner's input
    */
   private void refill(final int input) {
      byte[] value = this.inputs[input].hasNext() ? this.inputs[input].next() : null;
      int origin = input;

      int pos = (input + this.inputs.length) / 2;
      while (pos > 0) {
         final byte[] other = this.tree[pos];
         final int otherOrigin = this.origins[pos];
         if (this.compare(origin, value, otherOrigin, other) > 0) {
            this.tree[pos] = value;
            this.origins[pos] = origin;
            value = other;
            origin = otherOrigin;
         }
         pos /= 2;
      }
      this.tree[pos] = value;
      this.origins[pos] = origin;
   }

   @Override
   public void reset() {
      for (final TupleIterator iter : this.inputs) {
         iter.reset();
      }
      this.fill();
   }

   @Override
   public void close() {
      for (final TupleIterator iter : this.inputs) {
         iter.close();
      }
   }
}

/**
 * Implementation of the replacement sort algorithm for generating long initial runs
 * given a fixed amount of working memory.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
final class ReplacementSorter {
   /** Buffer manager. */
   private final BufferManager bufferManager;

   /** Size of the working memory in records. */
   private final int size;
   
   /** Record comparator. */
   private final RecordComparator comparator;
   
   /** Length of the records. */
   private final int recordSize;

   /**
    * Creates a replacement sorter.
    *
    * @param bufferManager buffer manager
    * @param size size of the working memory in records
    * @param comparator record comparator
    * @param recordSize length of the records
    */
   ReplacementSorter(final BufferManager bufferManager, final int size, final RecordComparator comparator,
         final int recordSize) {
      this.bufferManager = bufferManager;
      this.size = size;
      this.comparator = comparator;
      this.recordSize = recordSize;
   }

   /**
    * Creates initial runs by using the working memory as a binary heap of records
    * belonging to the current run, and a list of records that have to wait until the next one.
    *
    * @param input input iterator
    * @return runof initial runs
    */
   Run initialRuns(final TupleIterator input) {
      final RunBuilder runOfRuns = new RunBuilder(this.bufferManager, ExternalSort.RUN_OF_RUNS.getLength());
      final byte[][] buffer = new byte[this.size][];

      // fill the buffer initially
      int n = 0;
      while (n < buffer.length && input.hasNext()) {
         buffer[n++] = input.next();
      }

      while (n > 0) {
         // produce another run
         this.heapify(buffer, 0, n);
         int nextRun = 0;

         final RunBuilder builder = new RunBuilder(this.bufferManager, this.recordSize);
         while (n > 0) {
            final byte[] out = buffer[0];
            builder.appendRecord(out);
            if (--n > 0) {
               buffer[0] = buffer[n];
               buffer[n] = null;
               this.heapifyDown(buffer, 0, n);
            }

            if (input.hasNext()) {
               final byte[] next = input.next();
               if (this.comparator.compare(out, next) <= 0) {
                  // next record belongs to the current run
                  buffer[n++] = next;
                  this.heapifyUp(buffer, n - 1);
               } else {
                  // next record belongs to the next run
                  buffer[buffer.length - 1 - nextRun] = next;
                  nextRun++;
               }
            }
         }

         final Run run = builder.finish();
         final byte[] runRecord = ExternalSort.RUN_OF_RUNS.newTuple();
         ExternalSort.RUN_OF_RUNS.setIntField(runRecord, 0, run.getFirstPageID().getValue());
         ExternalSort.RUN_OF_RUNS.setBigintField(runRecord, 1, run.getLength());
         runOfRuns.appendRecord(runRecord);

         if (nextRun > 0) {
            final int gap = buffer.length - nextRun;
            if (gap > 0) {
               System.arraycopy(buffer, gap, buffer, 0, nextRun);
            }
            n = nextRun;
         }
      }
      return runOfRuns.finish();
   }

   /**
    * Swaps two objects in the given array.
    *
    * @param values array to swap the elements in
    * @param i index of the first element
    * @param j index of the second element
    */
   private static void swap(final Object[] values, final int i, final int j) {
      final Object temp = values[i];
      values[i] = values[j];
      values[j] = temp;
   }

   /**
    * Establishes heap invariants in the given range.
    * @param values array
    * @param start start index
    * @param end end index
    */
   private void heapify(final byte[][] values, final int start, final int end) {
      final int lastParent = (end - 1) / 2;
      for (int i = lastParent; i >= start; i--) {
         this.heapifyDown(values, i, end);
      }
   }

   /**
    * Propagates a value from the root of the heap down.
    *
    * @param values array
    * @param start position of the heap's root
    * @param end end of the heap
    */
   private void heapifyDown(final byte[][] values, final int start, final int end) {
      int pos = start;
      while (2 * pos < end - 1) {
         final int left = 2 * pos + 1;
         final int child = left + 1 == end
               || this.comparator.compare(values[left], values[left + 1]) <= 0 ? left : left + 1;
         if (this.comparator.compare(values[pos], values[child]) <= 0) {
            break;
         }
         swap(values, pos, child);
         pos = child;
      }
   }

   /**
    * Propagates a value from the bottom of the heap up.
    *
    * @param buffer array
    * @param pos index of the value to propagate up
    */
   private void heapifyUp(final byte[][] buffer, final int pos) {
      for (int curr = pos, par; curr > 0; curr = par) {
         par = (curr - 1) / 2;
         if (this.comparator.compare(buffer[par], buffer[curr]) <= 0) {
            break;
         }
         swap(buffer, curr, par);
      }
   }
}

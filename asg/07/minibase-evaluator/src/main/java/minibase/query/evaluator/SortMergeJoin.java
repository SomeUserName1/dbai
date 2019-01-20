package minibase.query.evaluator;

import java.util.NoSuchElementException;

import minibase.query.evaluator.compare.RecordComparator;
import minibase.query.evaluator.compare.TupleComparator;
import minibase.query.schema.Schema;
import minibase.storage.buffer.BufferManager;

/**
 * Sl 24 ff.
 */
public class SortMergeJoin extends AbstractOperator {
   /** Outer input relation. */
   private final Operator outer;

   /** Inner input relation. */
   private final Operator inner;

   /** Join predicate. */
   private final RecordComparator predicate;



   /**
    * Constructor setting the schema.
    *
    *
    */
   SortMergeJoin(final BufferManager bufferManager, final Operator outer, final int outerColumn,
         final Operator inner, final int innerColumn) {
      // TODO implement
      super(Schema.join(outer.getSchema(), inner.getSchema()));
      this.outer = outer;
      this.inner = inner;
      this.predicate = new TupleComparator(outer.getSchema(), new int[] { outerColumn },
            inner.getSchema(), new int[] { innerColumn });
   }

   @Override public TupleIterator open() {
      // TODO implement
      final TupleIterator outer = this.outer.open();
      if (!outer.hasNext()) {
         outer.close();
         return TupleIterator.EMPTY;
      }
      final TupleIterator inner = this.inner.open();
      if (!inner.hasNext()) {
         outer.close();
         inner.close();
         return TupleIterator.EMPTY;
      }

      return new TupleIterator() {

         /** Current tuple from the iterator of the outer relation. */
         private byte[] currentOuter = outer.next();

         /** Next tuple to return. */
         private byte[] next;

         @Override
         public boolean hasNext() {
            // TODO implement
            if (this.next != null) {
               return true;
            }

            for (;;) {
               if (this.currentOuter != null) {
                  while (inner.hasNext()) {
                     final byte[] currentInner = inner.next();
                     if (SortMergeJoin.this.predicate.equals(this.currentOuter, currentInner)) {
                        this.next = Schema.join(this.currentOuter, currentInner);
                        return true;
                     }
                  }
                  this.currentOuter = null;
               }
               if (!outer.hasNext()) {
                  return false;
               }
               this.currentOuter = outer.next();
               inner.reset();
            }
         }

         @Override
         public byte[] next() {
            // TODO implement
            // validate the next tuple
            if (!this.hasNext()) {
               throw new NoSuchElementException("No more tuples to return.");
            }
            // return (and forget) the tuple
            final byte[] tuple = this.next;
            this.next = null;
            return tuple;
         }

         @Override
         public void reset() {
            // TODO implement
            outer.reset();
            inner.reset();
            this.currentOuter = null;
            this.next = null;
         }

         @Override
         public void close() {
            // TODO implement
            outer.close();
            inner.close();
            this.currentOuter = null;
            this.next = null;
         }
      };
   }
}

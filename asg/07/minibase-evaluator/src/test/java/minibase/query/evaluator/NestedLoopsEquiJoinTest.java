package minibase.query.evaluator;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

import minibase.access.file.HeapFile;

/**
 * @author Manuel Hotz &lt;manuel.hotz&gt;
 * @since 1.0
 */
public class NestedLoopsEquiJoinTest extends EvaluatorBaseTest {

   @Test
   public void testEmpty() {
      // test join relation with empty relation
      try (HeapFile f = HeapFile.createTemporary(this.getBufferManager());
            HeapFile o = HeapFile.createTemporary(this.getBufferManager())) {
         byte[] data = S_SAILORS.newTuple();
         S_SAILORS.setAllFields(data, 1, "c", 1, 1.0f);
         f.insertRecord(data);

         NestedLoopsEquiJoin nlj = new NestedLoopsEquiJoin(this.getBufferManager(),
               new TableScan(S_SAILORS, f), 0, new TableScan(S_SAILORS, o), 0);
         try (TupleIterator it = nlj.open()) {
            assertFalse(it.hasNext());
         }
      }
   }
}

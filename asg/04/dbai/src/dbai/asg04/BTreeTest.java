package dbai.asg04;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This class tests the BTreeGroup00 implementation.
 * 
 * The tests provided here are not sufficient to show correctness, and completing them
 * without failures does not guarantee a good grade. Testing your solution is part of
 * this exercise.
 *
 * @author Leo Woerteler, DBIS, University of Konstanz
 */
public final class BTreeTest {
   /** BTreeGroup00 instance. */
   private AbstractBTree tree;

   /** BitSet to compare the BTreeGroup00 with. */
   private BitSet cmp = new BitSet();

   /** Resets BTreeGroup00 and BitSet. */
   @BeforeEach
   public void buildUp() {
      // TODO you can experiment with the node size and/or write additional test methods
      this.tree = new BTreeGroup05(2);
      this.cmp.clear();
   }

   /**
    * Checks if after inserting and removing 10 million values the tree is empty again.
    */
   @Test
   public void testFillEmpty() {
      final Random rng = new Random(42);
      final int n = 200;
      final int[] rand = new int[n];
      for (int i = 0; i < rand.length; i++) {
         rand[i] = rng.nextInt(n) + 1;
          System.out.println("-----Iteration: " + i + " To be inserted: " + rand[i] + "----------------");
         this.tree.insert(rand[i]);
         this.cmp.set(rand[i]);
         //if (i % (n / 10) == 0) {

          System.out.println("Insertion completed!");
          this.tree.printChildren();
          System.out.println(this.tree.toString());
          this.tree.checkInvariants();
         //   System.out.println(this.tree.getStatistics());
         //}
      }
      this.tree.checkInvariants();
      assertEquals(this.cmp.cardinality(), this.tree.size());
      for (final int i : rand) {
         this.tree.delete(i);
      }
      assertEquals(0, this.tree.size());
   }

   /** Executes 1 million random operations on a B+-tree with 10'000 distinct keys. */
   @Test
   public void smallFuzzyTest() {
      this.fuzzyTest(1000000, 10000);
   }

   /**
    * Executes the given number of random operations on a B+-tree with the given number of distinct keys.
    * @param ops number of operations
    * @param numKeys number of distinct keys
    */
   private void fuzzyTest(final int ops, final int numKeys) {
      final Random rand = new Random(ops);
      for (int i = 0; i < ops; i++) {
         if ((i + 1) % 100000 == 0) {
            System.out.println(i + 1 + " operations:");
            System.out.println(this.tree.getStatistics());
            this.tree.checkInvariants();
         }
         final int rnd = rand.nextInt(numKeys);
         final int key = rnd - numKeys / 2;
         final int nextOp = rand.nextInt(3);
         if (nextOp == 0) {
            // insert value
            this.cmp.set(rnd);
            this.tree.insert(key);
            assertTrue(this.tree.contains(key), "insertion of " + key + " failed");
         } else if (nextOp == 1) {
            // random key lookup
            assertEquals(this.cmp.get(rnd), this.tree.contains(key));
         } else {
            // delete key
            this.cmp.clear(rnd);
            this.tree.delete(key);
            assertFalse(this.tree.contains(key), "deletion of " + key + " failed");
            assertEquals(this.cmp.cardinality(), this.tree.size());
         }
      }
      assertEquals(this.cmp.cardinality(), this.tree.size());
      for (int i = this.cmp.nextSetBit(0); i >= 0; i = this.cmp.nextSetBit(i + 1)) {
         this.tree.delete(i - numKeys / 2);
      }
      assertEquals(0, this.tree.size());
   }
}

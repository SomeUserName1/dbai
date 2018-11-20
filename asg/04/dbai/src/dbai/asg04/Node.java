package dbai.asg04;

import java.util.Arrays;

/**
 * Representation of a B+ Tree node.
 *
 * @author Leo Woerteler, DBIS, University of Konstanz
 */
final class Node {
   /** Keys in inner nodes, stored values in leaves. */
   private final int[] keys;

   /** Node array, pointing to the child nodes. This array is not initialized for leaf nodes. */
   private final int[] children;

   /** Number of entries (Rule in B+ Trees: d <= size <= 2 * d). */
   private int size = 0;

   /**
    * Constructor.
    *
    * @param maxSize maximum allowed size of this node
    * @param leaf if this is a leaf node
    */
   Node(final int maxSize, final boolean leaf) {
      this.keys = new int[maxSize];
      if (leaf) {
         this.children = null;
      } else {
         this.children = new int[maxSize + 1];
         Arrays.fill(this.children, -1);
      }
   }

   /**
    * Checks is this is a leaf node.
    *
    * @return result of check
    */
   public boolean isLeaf() {
      return this.children == null;
   }

   /**
    * Returns the array of keys of this node.
    *
    * @return array of keys
    */
   public int[] getKeys() {
      return this.keys;
   }

   /**
    * Gets the key at the given position.
    *
    * @param pos position of the key
    * @return the key
    */
   public int getKey(final int pos) {
      return this.keys[pos];
   }

   /**
    * Sets the given key at the given position in the keys array.
    *
    * @param pos position of the key
    * @param key the key to set
    */
   protected void setKey(final int pos, final int key) {
      this.keys[pos] = key;
   }

   /**
    * Returns the array of child IDs of this node.
    *
    * @return array of child IDs
    */
   public int[] getChildren() {
      return this.children;
   }

   /**
    * Gets the child ID at the given position.
    *
    * @param pos position of the child ID
    * @return the child ID
    */
   public int getChildID(final int pos) {
      return this.children[pos];
   }

   /**
    * Sets the given child ID at the given position in the child ID array.
    * 
    * @param pos position of the child ID
    * @param childID child ID to set
    */
   protected void setChildID(final int pos, final int childID) {
      this.children[pos] = childID;
   }

   /**
    * Getter for the number of keys stored in this node.
    * @return number of keys
    */
   public int getSize() {
      return this.size;
   }

   /**
    * Setter for the number of keys stored in this node.
    * @param size new number of keys
    */
   protected void setSize(final int size) {
      this.size = size;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder();
      if (this.isLeaf()) {
         // leaf node
         sb.append("Leaf[");
         for (int i = 0; i < this.size; i++) {
            if (i != 0) {
               sb.append(", ");
            }
            sb.append(this.getKey(i));
         }
         sb.append("]");
      } else {
         sb.append("Branch[");
         sb.append('(').append(this.getChildID(0)).append(')');
         for (int i = 0; i < this.size; i++) {
            sb.append(", ").append(this.getKey(i)).append(", (")
                  .append(this.getChildID(i + 1)).append(')');
         }
         sb.append("]");
      }
      return sb.toString();
   }
}

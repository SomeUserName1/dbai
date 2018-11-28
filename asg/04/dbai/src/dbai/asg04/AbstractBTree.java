package dbai.asg04;

import java.util.*;

/**
 * Main memory based B+ Tree implementation.
 * 
 * @author Leo Woerteler, DBIS, University of Konstanz
 */
abstract class AbstractBTree {
   /** Constant for propagating that no more structural changes are necessary. */
   static final long NO_CHANGES = -1;

   /** Minimum number of keys in a node. */
   private final int minSize;
   /** Fixed Node Size. */
   private final int maxSize;

   /** Map from node IDs to nodes. */
   private final Map<Integer, Node> nodes = new HashMap<Integer, Node>();
   /** Counter for node IDs. */
   private int nextID;

   /** Pointer to the root node. */
   private int root;
   /** Number of currently stored keys. */
   private int size;

   /**
    * Constructor specifying the minimum number of keys per node.
    * 
    * @param d
    *           minimum number of keys per node
    */
   AbstractBTree(final int d) {
      this.minSize = d;
      this.maxSize = 2 * d;
      this.nextID = 1;
      this.setRoot(this.createNode(true));
   }

   private void printChildren(int nodeID) {
      Node node = this.getNode(nodeID);
      int[] childIDs = node.getChildren() == null ? new int[]{-1} : node.getChildren();
      System.out.println("NodeID: " + nodeID + "| Size: " + node.getSize() + "| childIDs" + Arrays.toString(childIDs) );

      int[] children = node.getChildren();
      if(children == null) return;
      for(int childNodeID : children) {
          if (childNodeID == -1) return;
         printChildren(childNodeID);

      }
   }

   public void printChildren() {
      int rootID = this.getRoot();
      System.out.println("Root Node ID: " + rootID);
      printChildren(rootID);
   }


   /**
    * Checks if the specified key exists in the tree.
    * 
    * @param key
    *           key to be found
    * @return result of check
    */
   public final boolean contains(final int key) {
      return this.containsKey(this.getRoot(), key);
   }

   /**
    * Inserts a key into the tree structure.
    * 
    * @param key
    *           key to be added
    */
   public final void insert(final int key) {
      final long res = this.insertKey(this.getRoot(), key);
      if (res != NO_CHANGES) {
         // the root node was split, create a new inner node and make it the root node
         // this is the only case in which the height of the tree increases
          System.out.println("Do we even split the root?");
         final int rootID = this.createNode(false);
         final Node rootNode = this.getNode(rootID);
         rootNode.setChildID(0, this.root);
         rootNode.setKey(0, getMidKey(res));
         rootNode.setChildID(1, getChildID(res));
         rootNode.setSize(1);
         this.setRoot(rootID);
      }
   }

   /**
    * Deletes a value from the tree structure.
    * 
    * @param value
    *           value to be removed
    */
   public final void delete(final int value) {
      this.deleteKey(this.root, value);
   }

   /**
    * Returns the number of currently stored keys.
    * 
    * @return number of stored keys
    */
   public final int size() {
      return this.size;
   }

   /**
    * Checks if the specified key is found in or below the specified node.
    * 
    * @param nodeID
    *           ID of the current node
    * @param key
    *           key to be found
    * 
    * @return {@code true} if the key was found, {@code false} otherwise
    */
   protected abstract boolean containsKey(int nodeID, int key);

   /**
    * Inserts a key into the node with the given ID.
    * 
    * @param nodeID
    *           ID of the node to insert into
    * @param key
    *           key to insert
    * @return if the node was split while inserting the given key, the return
    *         value is a pair (as created by {@link #keyIDPair(int, int)}) of the
    *         key between the old and the new node that has to be inserted into
    *         the parent and the ID of the newly created node, otherwise
    *         {@link #NO_CHANGES} is returned
    */
   protected abstract long insertKey(int nodeID, int key);

   /**
    * Deletes the given key from the node with the given ID if it is contained.
    * 
    * @param nodeID
    *           ID of the node to delete the key from
    * @param key
    *           the key to delete
    * @return {@code false} if the node is under-full as a result of the
    *         deletion, {@code true} otherwise
    */
   protected abstract boolean deleteKey(int nodeID, int key);

   /**
    * Packs the middle key and the newly allocated node's ID into one
    * {@code long} value. This cannot produce {@link #NO_CHANGES} because
    * {@code nodeID} does not have its highest-order bit set.
    * 
    * @param midKey
    *           the middle key
    * @param nodeID
    *           the node ID
    * @return the resulting {@code long} value
    */
   static final long keyIDPair(final int midKey, final int nodeID) {
      return ((long) midKey << 32) | nodeID;
   }

   /**
    * Extracts the middle key from the result of a call to
    * {@link #insertKey(int, int)}.
    * 
    * @param keyIDPair
    *           the pair of integers
    * @return the middle key
    */
   static final int getMidKey(final long keyIDPair) {
      return (int) (keyIDPair >>> 32);
   }

   /**
    * Extracts the child ID from the result of a call to
    * {@link #insertKey(int, int)}.
    * 
    * @param keyIDPair
    *           the pair of integers
    * @return the child ID
    */
   static final int getChildID(final long keyIDPair) {
      return (int) keyIDPair;
   }

   /**
    * Gets the minimum number of keys allowed in a node.
    * 
    * @return minimum number of keys
    */
   final int getMinSize() {
      return this.minSize;
   }

   /**
    * Returns the maximum number of keys allowed in a node.
    * 
    * @return maximum number of keys
    */
   final int getMaxSize() {
      return this.maxSize;
   }

   /**
    * Increments the number of keys stored in this tree, has to be called
    * whenever a key is inserted into a leaf node.
    */
   final void incrementSize() {
      this.size++;
   }

   /**
    * Decrements the number of keys stored in this tree, has to be called
    * whenever a key is deleted from a leaf node.
    */
   final void decrementSize() {
      this.size--;
   }

   /**
    * Getter for the root node ID.
    * 
    * @return the root node ID
    */
   final int getRoot() {
      return this.root;
   }

   /**
    * Setter for the root node ID.
    * 
    * @param newRoot
    *           new root node ID
    */
   final void setRoot(final int newRoot) {
      this.root = newRoot;
   }

   /**
    * Creates a new node and returns its ID.
    * 
    * @param leaf
    *           {@code true} for leaf node, {@code false} for inner node
    * 
    * @return node pointer
    */
   final int createNode(final boolean leaf) {
      final int nodeID = this.nextID++;
      this.nodes.put(nodeID, new Node(this.maxSize, leaf));
      return nodeID;
   }

   /**
    * Gets the node with the given ID.
    * 
    * @param nodeID
    *           node position
    * @return the node
    */
   final Node getNode(final int nodeID) {
      final Node node = this.nodes.get(nodeID);
      if (node == null) {
         throw new IllegalArgumentException("The node with ID " + nodeID + " does not exist.");
      }
      return node;
   }

   /**
    * Removes a node.
    * 
    * @param nodeID
    *           node to be removed
    */
   final void removeNode(final int nodeID) {
      if (this.nodes.remove(nodeID) == null) {
         throw new IllegalArgumentException("The node with ID " + nodeID + " does not exist.");
      }
   }

   /**
    * Checks if all invariants hold for this B+ Tree.
    * 
    * @throws AssertionError
    *            if invariants are violated
    */
   public final void checkInvariants() {
      final int size = this.checkInvariants(this.getRoot(),
            Integer.MIN_VALUE, (long) Integer.MAX_VALUE + 1);
      if (size != this.size()) {
         throw new AssertionError("Wrong size: " + this.size() + " != " + size);
      }
   }

   /**
    * Recursive helper method for {@link #checkInvariants()}.
    * 
    * @param nodeID
    *           current node ID
    * @param minIncl
    *           minimum allowed key in the current node (inclusive)
    * @param maxExcl
    *           maximum allowed key in the current node (exclusive)
    * @return number of entries in the sub-tree under the current node
    */
   private int checkInvariants(final int nodeID, final long minIncl, final long maxExcl) {
      final Node node = this.getNode(nodeID);
      final int keys = node.getSize();
      if (node.isLeaf()) {
         // check leaf node
         if (nodeID != this.getRoot() && keys < 1) {
            System.out.println(this.toString());
            throw new AssertionError("Non-root node " + nodeID
                  + " should not be empty.");
         }

         int before = node.getKey(0);
         if (before < minIncl) {
            System.out.println(this.toString());
            throw new AssertionError("Wrong key found, " + before
                  + " not in allowed range " + "[" + minIncl + "; " + maxExcl
                  + ").");
         }
         for (int i = 1; i < keys; i++) {
            final int key = node.getKey(i);
            if (key <= before) {
               System.out.println(this.toString());
               throw new AssertionError("Wrong key found, " + key
                     + " must be greater than preceding key " + before);
            }
            before = key;
         }
         if (before >= maxExcl) {
            System.out.println(this.toString());
            throw new AssertionError("Wrong key found, " + before
                  + " not in allowed range " + "[" + minIncl + "; " + maxExcl
                  + ").");
         }
         return keys;
      }

      // check branch node
      if (keys < 0) {
         throw new AssertionError("Node should not be empty: " + nodeID);
      }
      long left = minIncl;
      long right = keys == 0 ? maxExcl : node.getKey(0);
      int actualSize = 0;
      for (int i = 0; i <= keys; i++) {
         if (left >= right) {
            System.out.println(this.toString());
            throw new AssertionError("Invalid key range in node " + nodeID
                  + ": [" + left + "; " + right + ")");
         }

         // check the descendants recursively
         final int childID = node.getChildID(i);
         actualSize += this.checkInvariants(childID, left, right);
         left = right;
         right = i < keys - 1 ? node.getKey(i + 1) : maxExcl;
      }
      return actualSize;
   }

   @Override
   public final String toString() {
      final StringBuilder sb = new StringBuilder("BTree[\n");
      this.toString(sb, this.root, 1);
      return sb.append("]").toString();
   }

   /**
    * Recursive helper method for {@link #toString()}.
    * 
    * @param sb
    *           builder for the string representation
    * @param nodeID
    *           current node's ID
    * @param indent
    *           indentation depth
    */
   private void toString(final StringBuilder sb, final int nodeID,
         final int indent) {
      final Node node = this.getNode(nodeID);
      for (int i = 0; i < indent; i++) {
         sb.append("  ");
      }

      if (node.isLeaf()) {
         sb.append("Leaf(").append(nodeID).append(")[");
         for (int i = 0; i < node.getSize(); i++) {
            if (i > 0) {
               sb.append(", ");
            }
            sb.append(node.getKey(i));
         }
      } else {
         sb.append("Branch(").append(nodeID).append(")[\n");
         this.toString(sb, node.getChildID(0), indent + 2);
         for (int i = 0; i < node.getSize(); i++) {
            for (int j = 0; j < indent + 1; j++) {
               sb.append("  ");
            }
            sb.append(node.getKey(i)).append('\n');
            this.toString(sb, node.getChildID(i + 1), indent + 2);
         }
         for (int i = 0; i < indent; i++) {
            sb.append("  ");
         }
      }
      sb.append("]\n");
   }

   /**
    * Returns a string containing some statistics about this index.
    * @return the description
    */
   public String getStatistics() {
      final List<Double> vals = new ArrayList<Double>();
      this.getSpaceUsage(this.getRoot(), vals);
      double min = Double.MAX_VALUE;
      double sum = 0;
      double max = -Double.MAX_VALUE;
      int maxH = 0;

      final int buckets = 20;
      final int[] histo = new int[buckets];
      for (final double d : vals) {
         min = Math.min(min, d);
         sum += d;
         max = Math.max(max, d);
         final int h = ++histo[Math.min((int) (buckets * d), buckets - 1)];
         maxH = Math.max(maxH, h);
      }

      final StringBuilder sb = new StringBuilder();
      sb.append(" - ").append(this.nodes.size()).append(" nodes\n");
      sb.append(" - ").append(this.size()).append(" entries\n");
      sb.append(String.format(" - %.2f%% %s page space used\n", 100 * min, "minimum"));
      sb.append(String.format(" - %.2f%% %s page space used\n", 100 * sum / vals.size(), "average"));
      sb.append(String.format(" - %.2f%% %s page space used\n", 100 * max, "maximum"));

      sb.append(" - Histogram: [");
      for (final int h : histo) {
         final int cp = h == 0 ? '\u2591' : '\u2581' + Math.min((int) (8.0 * h / maxH), 7);
         sb.append((char) cp);
      }
      return sb.append("]").toString();
   }

   /**
    * Gathers the space usage of all pages recursively.
    * @param pageID the current page's ID
    * @param vals list to append to
    */
   private void getSpaceUsage(final int pageID, final List<Double> vals) {
      final Node page = this.getNode(pageID);
      final int numKeys = page.getSize();

      if (pageID != this.getRoot()) {
         vals.add(1.0 * numKeys / this.maxSize);
      }

      if (!page.isLeaf()) {
         for (int i = 0; i <= numKeys; i++) {
            final int childID = page.getChildID(i);
            this.getSpaceUsage(childID, vals);
         }
      }
   }
}

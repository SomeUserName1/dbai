package dbai.asg04;

public final class BTreeGroup00 extends AbstractBTree {
    public BTreeGroup00(fina l int d) {
      super(d);
   } 
    
   @Override
   protected boolean containsKey(final int nodeID, final int key) {
       int[] searchPath = this.searchNode(nodeID, key);
       Node mNode = this.getNode(searchPath[searchPath.length-1]);
       
       /* loop over the keys in the child and return true if it's present */
       for (int leaf_keys : mNode.getKeys()) if(key == leaf_key) return true;
       /* if we looped over all keys but didnt find anything, the key is not present */
       return false;
   }

   @Override
   protected long insertKey(final int nodeID, final int key) {
       int[] searchPath = this.searchNode(nodeID, key);
       Node mNode = this.getNode(searchPath[searchPath.length-1]);
       
       /* if the key is present skip the insertion according to the exercise description */
       if(this.containsKey(nodeID, key)) return NO_CHANGES;
       
       /* if there is space left ... */ 
       int nodeSize = mNode.getSize();
       if(nodeSize < this.getMaxSize()) {
           /* and the key is larger than the current largest key in the node append the key */
           if(key > mNode.getKey(nodeSize-1)) {
               mNode.setKey(nodeSize, key);
               mNode.setSize(++nodeSize);
               return NO_CHANGES;
           } else { /* else look for the next larger key and insert */
               int[] leaf_keys = mNode.getKeys();
               int i = 0;
               for (int i=0; i < leaf_keys.length; ++i) {
                   if(key < leaf_keys[i]) {
                       /* insert the key */
                        mNode.setKey(i, key);
                        /* shift all other keys to the right */
                        for (i; i < leaf_keys.length; ++i) mNode.setKey(i+1, leaf_keys[i]);
                        mNode.setSize(++nodeSize);
                        return NO_CHANGES;
                   }
               }
           }
       } else { /* Split the child */

           // TODO Check neighbours and redistribute up to d nodes  
           int[] splitKeys = mNode.getKeys();
           int newNodeID = this.createNode(true);
           Node newNode = this.getNode(newNodeID);
           
           int j = 0; 
           for (int i=this.getMinSize(); i<mNode.getSize(); ++i){
             newNode.setKey(j, splitKeys[i]);
             newNode.setSize(++j);
             mNode.setKey(i, 0);
             mNode.setSize(mNode.getSize()-1);
           }
           return propagateSplit(searchPath, newNodeID);
       }    
   }

   @Override
   protected boolean deleteKey(final int nodeID, final int key) {
       int[] searchPath = this.searchNode(nodeID, key);
       Node mNode = this.getNode(searchPath[searchPath.length-1]);

       // TODO implement this
      throw new UnsupportedOperationException("Not yet implemented.");
   }


   private int[] searchNode(final nodeID, final int key) {
     Node mNode = this.getNode(nodeID);
       if(mNode.isLeaf()){
       /* if we reached here we selected the corresponding leaf and return its ID */
           return nodeID;
       } else {
       /* if the node is not a leaf, ... */
           /*  check weather it is larger than the largest key of the current node.*/
           /* If so the location of the desired node must be in the subtree that the */
           /* last child is pointing to */
           if (key >= mNode.getKey(mNode.getSize()-1)) 
               return combine(nodeID, this.searchNode(mNode.getChild(mNode.getSize()), key));
           /* else check the key other keys to be larger and descend recursive accordingly */
           int i = 0;
           for (int node_key : mNode.getKeys()) { 
               /* check if key can be found in left child */
               if(key < nkey) return combine(nodeID, this.searchNode(mNode.getChild(i), key));
               ++i;
           }
       }
   }

   private long propagateSplit(int[] searchPath, int newNodeID) {
       int parentID = searchPath[searchPath.length-2];
       Node parent = this.getNode(parentID);
       int parentSize = parent.getSize(); 
       if(!(parentSize < this.getMaxSize())) { 
           if(parent == this.root)
                return this.keyIDPair(splitKeys[this.getMinSize()], newNodeID);

           int[] parentKeys = parent.getKeys();
           int[] parentChildren = parent.getChildren();
           int insertKey = this.getNode(newNodeID).getKey(0);
           int insertNodeID = newNodeID;

           // TODO !!! ERROR SOURCE: not correct
           int midKey = 0;
           int tempKey = 0; 
           int tempChild = 0;
           for (int i=0; i<parentKeys.length; i++) {
               if (insertKey < parentKeys[i] ) {
                   tempKey = parentKeys[i];
                   tempChild = parentChildren[i+1];
                   parentKeys[i] = insertKey;
                   parentChildren[i+1] = insertNodeID;
                   insertKey = tempKey;
                   insertNodeID = tempChild;
               }
               if (i == this.getMinSize()) {
                   midKey = parentKeys[i];
                   parentKeys[i] = insertKey < parentKeys[i+1] ? insertKey : parentKey[i+1];
               }
           }

           int splitNodeID = this.createNode(false);
           Node splitNode = this.getNode(insertNodeID);

           int j = 0; 
           for (int i=this.getMinSize(); i<parent.getSize(); ++i){
             newNode.setChildID(j, splitChildren[i+1]);
             mNode.setChildrenID(i+1, -1);
             newNode.setKey(j, splitKeys[i]);
             ++j;
             newNode.setSize(j);
             mNode.setKey(i, 0);
             mNode.setSize(mNode.getSize()-1);
           }
            

           return propagateSplit()
       } else { /* free space, adjust the key and ID to the first element in the new node */
            /* and its ID respectively */
            parent.setKey(parentSize, newNodeID.getKey(0));
            parent.setChildID(parentSize+1, newNodeID);

            return NO_CHANGES;
       }

   }
}

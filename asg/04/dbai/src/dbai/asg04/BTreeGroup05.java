package dbai.asg04;

import java.util.Arrays;

/* spaghetti code; size bugged, maybe more */

public final class BTreeGroup05 extends AbstractBTree {
	public BTreeGroup05(final int d) {
		super(d);
	}

	@Override
	protected boolean containsKey(final int nodeID, final int key) {
		int[] searchPath = this.searchLeaf(nodeID, key);
		Node mNode = this.getNode(searchPath[searchPath.length - 1]);
		
		if(!mNode.isLeaf()) return false;

		/* loop over the keys in the child and return true if it's present */
		for (int i = 0; i < mNode.getSize(); i++) {
			if(mNode.getKey(i) == key) return true;
		}
		
		/* if we looped over all keys but didnt find anything, the key is not present */
		return false;
	}

	@Override
	protected long insertKey(final int nodeID, final int key) {
		int[] searchPath = searchLeaf(nodeID, key);
		int mNodeID = searchPath[searchPath.length - 1];

		Node mNode = this.getNode(mNodeID);
		int nodeSize = mNode.getSize();
		/*
		 * if the key is present skip the insertion according to the exercise
		 * description
		 */
		if (this.containsKey(nodeID, key))
			return NO_CHANGES;

		/* if there is space left ... */
		if (nodeSize < this.getMaxSize()) {
			/*
			 * and the key is larger than the current largest key in the node append the key
			 */
			insert(mNodeID, key, -1);
			incrementSize();
			return NO_CHANGES;

		} else { /* Split the child */
         if( this.getRoot() != mNodeID)
            if(redistributeInsert(searchPath, key))
               return NO_CHANGES;
			int[] splitKeys = mNode.getKeys();
			int newNodeID = this.createNode(true);
			Node newNode = this.getNode(newNodeID);
			int splitNodeSize = nodeSize - this.getMinSize();
			int insertPosition = nodeSize;

			for (int i = 0; i < nodeSize; ++i) {
				if (key < splitKeys[i]) {
					insertPosition = i;
					break;
				}
			}

			/* copy the upper part over */
			System.arraycopy(splitKeys, this.getMinSize(), newNode.getKeys(), 0, splitNodeSize);

			newNode.setSize(splitNodeSize);
			mNode.setSize(this.getMinSize());

			if (insertPosition < this.getMinSize()) {
				insert(mNodeID, key, -1);
			} else {
				insert(newNodeID, key, -1);
			}
			incrementSize();

			if (mNodeID == this.getRoot()) {
				return keyIDPair(newNode.getKey(0), newNodeID);
			}
			return propagateSplit(searchPath, newNodeID, newNode.getKey(0));
		}
	}

	@Override
	protected boolean deleteKey(final int nodeID, final int key) {
		
		// get Path to Key
		int[] searchPath = this.searchLeaf(nodeID, key);
		Node mNode = this.getNode(searchPath[searchPath.length - 1]);
		
		int keys[] = mNode.getKeys();
		
		// find position in node
		int keyIndex = -1;
		for(int i = 0; i < mNode.getSize(); i++) {
			if (key == keys[i]) {
				keyIndex = i;
				break;
			}
		}
		
		if (keyIndex == -1) return false;
		
		// refactor Node
		System.arraycopy(keys, keyIndex + 1, keys, keyIndex, mNode.getSize() - keyIndex - 1);
		mNode.setSize(mNode.getSize() - 1);
		mNode.setKey(mNode.getSize(), 0);
		
		// if node is to small and not the root, merge it
		if (mNode.getSize() < this.getMinSize() && searchPath.length > 1) {
			propagateMerge(searchPath);
		}
		
		this.decrementSize();
		
		return true;
	}

	private static int[] concat(int[] a, int[] b) {
		int length = a.length + b.length;
		int[] result = new int[length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	private void insert(int nodeID, int key, int childID) {
		
		// find node to insert the Key into
		Node mNode = this.getNode(nodeID);
		int nodeSize = mNode.getSize();
		int[] keys = mNode.getKeys();
		int[] children = mNode.getChildren();
		
		if (nodeSize == 0) {
			mNode.setKey(nodeSize, key);
			mNode.setSize(nodeSize + 1);
		} else if (key > mNode.getKey(nodeSize - 1)) {

			mNode.setSize(nodeSize + 1);
			mNode.setKey(nodeSize, key);
			if (!mNode.isLeaf()) {
				mNode.setChildID(nodeSize + 1, childID);
			}
		} else { /* else look for the next larger key and insert */
			for (int i = 0; i < nodeSize; ++i) {
				if (key < keys[i]) {
					/* shift all other keys to the right */
					System.arraycopy(keys, i, keys, i + 1, nodeSize - i);
					if (!mNode.isLeaf()) {
						System.arraycopy(children, i + 1, children, i + 2, nodeSize - i);
						children[i + 1] = childID;
					}
					/* insert the key */
					mNode.setKey(i, key);
					mNode.setSize(nodeSize + 1);

					return;
				}
			}
		}
	}

	/**
	 * @param nodeID
	 *            the currently visited node
	 * @param key
	 *            the key to be found
	 * @return the search path including the leaf at position
	 *         searchPath[searchPath.length-1]
	 */
	private int[] searchLeaf(final int nodeID, final int key) {
		Node mNode = this.getNode(nodeID);
		if (mNode.isLeaf()) {
			/* if we reached here we selected the corresponding leaf and return its ID */
			return new int[] { nodeID };
		} else {
			/* if the node is not a leaf, ... */
			/* check weather it is larger than the largest key of the current node. */
			/* If so the location of the desired node must be in the subtree that the */
			/* last child is pointing to */
            int[] node_keys = mNode.getKeys();
			/* else check the other keys and find the next larger one */
			for (int i = 0; i < mNode.getSize(); i++) {
				/* check if key can be found in left child */
				if (key < node_keys[i]) {
					return concat(new int[] { nodeID }, this.searchLeaf(mNode.getChildID(i), key));
				}
			}
			return concat(new int[] { nodeID }, this.searchLeaf(mNode.getChildID(mNode.getSize()), key));
		}
	}

	/**
	 * @param searchPath
	 *            the path from the root to the according leaf or reduced path while
	 *            recursion
	 * @param newNodeID
	 *            the id of the child to be inserted
	 * @return NO_CHANGE or the key-id pair of the new node and the middle key
	 */
	private long propagateSplit(int[] searchPath, int newNodeID, int key) {
		
		/*
		 * get parent node of the leaf. the leaf is at searchPath[searchPath.length-1],
		 * so the parent is at len-2
		 */
		int parentID = searchPath[searchPath.length - 2];
		Node parent = this.getNode(parentID);
		int parentSize = parent.getSize();
		int parentKeys[] = parent.getKeys();
		int parentChildren[] = parent.getChildren();

		int insertPosition = parentSize;

		/* if there is no free space left */
		if (!(parentSize < this.getMaxSize())) {
		   /* find the mid key */
			/*
			 * if insertPosition doesn't change after the following for-loop it's the
			 * largest element
			 */

			int middle = this.getMinSize();

			for (int i = 0; i < parentSize; ++i) {
            if (key < parentKeys[i]) {
               insertPosition = i;
               break;
            }
         }

			int splitNodeID = createNode(false);
			Node splitNode = getNode(splitNodeID);

			int splitNodeKeys[] = splitNode.getKeys();
			int splitNodeChildren[] = splitNode.getChildren();
			
			if (insertPosition < middle) {

				// new Child is inserted into left part of the node

				// assemble right Node
				System.arraycopy(parentKeys, middle, splitNodeKeys, 0, this.getMinSize());
				System.arraycopy(parentChildren, middle + 1, splitNodeChildren, 1, this.getMinSize());

				Arrays.fill(parentKeys, middle, this.getMaxSize() - 1, 0);
				Arrays.fill(parentChildren, middle + 1, this.getMaxSize(), -1);

				parent.setSize(this.getMinSize());
				insert(parentID, key, newNodeID);

				splitNodeChildren[0] = parentChildren[middle + 1];
				
				parent.setSize(this.getMinSize());
				splitNode.setSize(this.getMinSize());

			} else if (insertPosition == middle) {
				
				// inserted Key needs to be moved one level up
				
				System.arraycopy(parentKeys, middle, splitNodeKeys, 0, this.getMinSize());
				System.arraycopy(parentChildren, middle + 1, splitNodeChildren, 1, this.getMinSize());
				
				Arrays.fill(parentKeys, middle, this.getMaxSize() - 1, 0);
				Arrays.fill(parentChildren, middle + 1, this.getMaxSize(), -1);
				
				parent.setSize(this.getMinSize());
				splitNode.setSize(this.getMinSize());
				
				splitNodeChildren[0] = newNodeID;
				parentKeys[middle] = key;
				
			} else {

				//  new Child is inserted into right part of the node

				System.arraycopy(parentKeys, middle + 1, splitNodeKeys, 0, this.getMinSize() -1);
				System.arraycopy(parentChildren, middle + 1, splitNodeChildren, 0, this.getMinSize());

				Arrays.fill(parentKeys, middle + 1, this.getMaxSize() -1, 0);
				Arrays.fill(parentChildren, middle + 1, this.getMaxSize(), -1);

				parent.setSize(this.getMinSize());
				splitNode.setSize(this.getMinSize() -1);

				insert(splitNodeID, key, newNodeID);

			}

			int middleKey = parentKeys[middle];
			parentKeys[middle] = 0;

			if (parentID == getRoot()) {
				return keyIDPair(middleKey, splitNodeID);
			}

			return propagateSplit(Arrays.copyOfRange(searchPath, 0, searchPath.length -1), splitNodeID, middleKey);

		} else {

			insert(parentID, key, newNodeID);
			return NO_CHANGES;
		}
	}


	private boolean redistributeInsert(int[] searchPath, int newKey){
		
	   int nodeID = searchPath[searchPath.length-1];
	   Node node = this.getNode(nodeID);
	   int[] keys = node.getKeys();

	   int parentID = searchPath[searchPath.length-2];
	   Node parent = this.getNode(parentID);
      int[] parentChildren = parent.getChildren();

      /* try to redistribute */
      int childPos = getChildPos(parentID, nodeID);
      int neighbourID;

      boolean left = false;
      if(childPos == 0) {
         neighbourID = parentChildren[1];
      } else  if (childPos == parent.getSize()) {
         neighbourID = parentChildren[parent.getSize() - 1];
         left=true;
      } else {
         if (getNode(parentChildren[childPos - 1]).getSize() >
               getNode(parentChildren[childPos + 1]).getSize()) {
            neighbourID = parentChildren[childPos + 1];
         } else {
            neighbourID = parentChildren[childPos - 1];
            left = true;
         }
      }

      Node neighbour = getNode(neighbourID);
      int neighbourKeys[] = neighbour.getKeys();

      // do redistribution
      if (neighbour.getSize() < this.getMaxSize()) {
         if (left) {
            int biggerKey = newKey > keys[0] ? newKey :keys[0];
            int smallerKey = biggerKey == newKey ? keys[0] : newKey;


            System.arraycopy(keys, 1, keys, 0, this.getMaxSize()-1);
            neighbour.setKey(neighbour.getSize(), smallerKey);
            node.setSize(node.getSize()-1);
            int[] newPath = searchLeaf(searchPath[searchPath.length-2], biggerKey);
            insert(newPath[newPath.length-1], biggerKey, -1);
            incrementSize();
            neighbour.setSize(neighbour.getSize()+1);

            parent.setKey(childPos-1, this.getNode(newPath[newPath.length-1]).getKey(0));

            return true;
         } else {
            int biggerKey = newKey > keys[this.getMaxSize()-1] ? newKey : keys[this.getMaxSize()-1];
            int smallerKey = biggerKey == newKey ? keys[this.getMaxSize()-1] : newKey;


            System.arraycopy(neighbourKeys, 0, neighbourKeys, 1, neighbour.getSize());
            neighbour.setKey(0, biggerKey);
            node.setSize(node.getSize()-1);
            int[] newPath = searchLeaf(searchPath[searchPath.length-2], smallerKey);
            insert(newPath[newPath.length-1], smallerKey, -1);
            incrementSize();
            neighbour.setSize(neighbour.getSize()+1);
            parent.setKey(childPos, neighbour.getKey(0));

            return true;
         }
      }
      return false;
   }

   private int getChildPos(int parentID, int nodeID){
	   Node parent = this.getNode(parentID);
	   int[] parentChildren = parent.getChildren();
      int childPos = 0;
      for (int i = 0; i <= parent.getSize(); i++) {
         if (parentChildren[i] == nodeID) {
            childPos = i;
            break;
         }
      }
      return childPos;
   }

	private void propagateMerge(int[] searchPath) {

		// get node and parent
		int nodeID = searchPath[searchPath.length -1];
		int parentID = searchPath[searchPath.length -2];
		Node parent = getNode(parentID);

		Node node = getNode(nodeID);
		int keys[] = node.getKeys();
		int children[] = node.getChildren();
		
		int parentChildren[] = parent.getChildren();
		int parentKeys[] = parent.getKeys();
	
		int childPos = 0;
		for (int i = 0; i <= parent.getSize(); i++) {
			if (parentChildren[i] == nodeID) {
				childPos = i;
				break;
			}
		}

		// find the smallest neighbor
		int neighbourIndex;

		boolean left = false;

		if(childPos == 0) {
			neighbourIndex = 1;
		} else  if (childPos == parent.getSize()) {
			neighbourIndex = parent.getSize() - 1;
			left = true;
		} else {
			if (getNode(parentChildren[childPos - 1]).getSize() <
				getNode(parentChildren[childPos + 1]).getSize()) {

				neighbourIndex = childPos + 1;
			} else {
				neighbourIndex = childPos - 1;
				left = true;
			}
		}
		
		// get neighbor
		int neighbourID = parentChildren[neighbourIndex];
		Node neighbour = getNode(neighbourID);
		int neighbourKeys[] = neighbour.getKeys();
		int neighbourChildren[] = neighbour.getChildren();

		// do redistribution
		if (neighbour.getSize() > this.getMinSize()) {
			
			// neighbor is left of node
			if (left) {
				
				// node is a leaf
				if (node.isLeaf()) {
				
					// duplicate smallest key of right node
					int stolenKey = neighbourKeys[neighbour.getSize() -1];
					
					neighbour.setSize(neighbour.getSize() -1);

					System.arraycopy(keys, 0, keys, 1, this.getMinSize() -1);
					keys[0] = stolenKey;
				

					node.setSize(this.getMinSize());
				
					parentKeys[childPos - 1] = stolenKey;
				
				// node is a inner node
				} else {
					
					// "rotate" keys through parent
					System.arraycopy(keys, 0, keys, 1, this.getMinSize() -1);
					
					keys[0] = parentKeys[childPos - 1];
					parentKeys[childPos - 1] = neighbourKeys[neighbour.getSize() -1];
					
					node.setSize(this.getMinSize());
					 
					
					int stolenChild = neighbourChildren[neighbour.getSize()];
					neighbourChildren[neighbour.getSize()] = -1;
					
					System.arraycopy(children, 0, children, 1, this.getMinSize());
					children[0] = stolenChild;
					
					neighbour.setSize(neighbour.getSize() -1);
				}

			// neighbor is right of node
			} else {
				
				// node is a leaf
				if (node.isLeaf()) {

					// duplicate smallest key of right node
					int stolenKey = neighbourKeys[0];
					System.arraycopy(neighbourKeys, 1, neighbourKeys, 0, neighbour.getSize() -1);
				
					neighbour.setSize(neighbour.getSize() - 1);

					keys[this.getMinSize() - 1] = stolenKey;
				
					node.setSize(this.getMinSize());

					parentKeys[childPos] = neighbourKeys[0];
				
				// node is a inner node
				} else {
						
					// "rotate" keys through parent
					keys[this.getMinSize() - 1] = parentKeys[childPos];
					parentKeys[childPos]  = neighbourKeys[0];
					
					int stolenChild = neighbourChildren[0];
					
					System.arraycopy(neighbourKeys, 1, neighbourKeys, 0, neighbour.getSize() -1);
					System.arraycopy(neighbourChildren, 1, neighbourChildren, 0, neighbour.getSize());
					
					
					neighbourChildren[neighbour.getSize()] = -1;
					
					children[node.getSize() + 1] = stolenChild;
					
					
					node.setSize(node.getSize() + 1);
					neighbour.setSize(neighbour.getSize() -1);
					
				}
			}
			
			return;
		}


		// No redistribution possible.
		// merge needed

		if (node.getSize() + neighbour.getSize() < this.getMaxSize()) {
			
			// neighbor is left of node
			if (left) {
				
				if (node.isLeaf()) {
					// copy keys of node to neighbor
					System.arraycopy(keys, 0, neighbourKeys, neighbour.getSize(), this.getMinSize() -1);
				} else {
					// copy keys of node to neighbor and leave place for the key from the parent node
					System.arraycopy(keys, 0, neighbourKeys, neighbour.getSize() + 1, this.getMinSize() -1);
					neighbourKeys[neighbour.getSize()] = parentKeys[childPos - 1];
				}

				if (!node.isLeaf()) {
					// copy children to neighbour node
					System.arraycopy(children, 0, neighbourChildren, neighbour.getSize() + 1, this.getMinSize());
				}

				if (childPos < parent.getSize()) { 
					// refactor Parent
					System.arraycopy(parentKeys, childPos, parentKeys, childPos - 1, parent.getSize() - childPos);
					System.arraycopy(parentChildren, childPos + 1, parentChildren, childPos, parent.getSize() - childPos);
				}
				parent.setSize(parent.getSize() -1);
				parent.setChildID(parent.getSize() + 1, -1);
				parent.setKey(parent.getSize(), 0);
				
		
				// set size of neighbor
				if (node.isLeaf()) {
					neighbour.setSize(node.getSize() + neighbour.getSize());
				} else {
					neighbour.setSize(node.getSize() + 1 + neighbour.getSize());
				}
	
				// remove node from the tree
				this.removeNode(nodeID);
				
				if (this.getRoot() == parentID) {
					// set new root if needed
					if( parent.getSize() < 1 ) this.setRoot(neighbourID);
					return;
				} else if(parent.getSize() < this.getMinSize()) {
					// merge parent if needed
					propagateMerge(Arrays.copyOfRange(searchPath, 0, searchPath.length - 1));
				}
				
				return;

			} else {
				
				// analog to left
				
				if (node.isLeaf()) {
					System.arraycopy(neighbourKeys, 0,  keys, this.getMinSize() - 1, neighbour.getSize());
				} else {
					System.arraycopy(neighbourKeys, 0, keys, this.getMinSize(), neighbour.getSize());
					keys[this.getMinSize() -1] = parentKeys[childPos];
				}
				
				if(childPos < parent.getSize() - 1)
				{
					System.arraycopy(parentKeys, childPos + 1, parentKeys, childPos, parent.getSize() - childPos - 1);
					System.arraycopy(parentChildren, childPos + 2, parentChildren, childPos + 1, parent.getSize() - childPos - 1);
				}
				parent.setSize(parent.getSize() -1);
				parent.setChildID(parent.getSize() + 1, -1);
				parent.setKey(parent.getSize(), 0);

				if (!node.isLeaf()) {
					System.arraycopy(neighbourChildren, 0, children, this.getMinSize(), neighbour.getSize() + 1);
				}

				if (node.isLeaf()) {
					node.setSize(node.getSize() + neighbour.getSize());
				} else {
					node.setSize(node.getSize() + 1 + neighbour.getSize());
				}
				
				this.removeNode(neighbourID);
				
				if (this.getRoot() == parentID) {
					if( parent.getSize() < 1 ) this.setRoot(nodeID);
					return;
				} else if(parent.getSize() < this.getMinSize()) {
					propagateMerge(Arrays.copyOfRange(searchPath, 0, searchPath.length - 1));
				}
				
				return;
			}

		} 
		
		// This statement should never be reached!
		throw new UnsupportedOperationException("Unsupported Operation");
	}
}

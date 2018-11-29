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

		/* loop over the keys in the child and return true if it's present */
		for (int leaf_key : mNode.getKeys())
			if (key == leaf_key)
				return true;
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
			// TODO Check neighbours and redistribute up to d nodes

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

			// Unnötig, da Werte mit index >= size ignoriert werden
			// System.arraycopy(new int[splitNodeSize], 0, splitKeys, this.getMinSize(),
			// splitNodeSize);

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
		int[] searchPath = this.searchLeaf(nodeID, key);
		Node mNode = this.getNode(searchPath[searchPath.length - 1]);

		// TODO implement this analog to insert
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	private static int[] concat(int[] a, int[] b) {
		int length = a.length + b.length;
		int[] result = new int[length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	private void insert(int nodeID, int key, int childID) {
		Node mNode = this.getNode(nodeID);
		int nodeSize = mNode.getSize();
		int[] keys = mNode.getKeys();
		int[] children = mNode.getChildren();

		if (nodeSize == 0) {
			mNode.setKey(nodeSize, key);
			mNode.setSize(nodeSize + 1);
		} else if (key > mNode.getKey(nodeSize - 1)) {
			System.out.println(mNode.getKey(nodeSize - 1));
			System.out.println(mNode.toString());
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
						System.arraycopy(children, i, children, i + 1, nodeSize - i + 1);
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

			/* else check the other keys and find the next larger one */
			int i = 0;
			for (int node_key : mNode.getKeys()) {
				/* check if key can be found in left children */
				if (key < node_key) {
					return concat(new int[] { nodeID }, this.searchLeaf(mNode.getChildID(i), key));
				}
				i++;
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
			int middle = this.getMaxSize();

			for (int i = 0; i < parentSize; ++i) {
				if (key < parentKeys[i]) {
					insertPosition = i;
					break;
				}
			}

			int splitNodeID = createNode(false);
			Node splitNode = getNode(splitNodeID);
			

			if (insertPosition < middle) {

				// new Child is inserted into left Node
				
				// assemble right Node
				System.arraycopy(parent.getKeys(), middle, splitNode.getKeys(), 0, this.getMinSize());
				System.arraycopy(parent.getChildren(), middle, splitNode.getChildren(), 0, this.getMinSize() + 1);
				
				Arrays.fill(parent.getKeys(), middle, this.getMaxSize() - 1, 0);
				Arrays.fill(parent.getChildren(), middle, this.getMaxSize(), -1);
				
				parent.setSize(this.getMinSize());
				insert(parentID, key, newNodeID);
				
				parent.setSize(this.getMinSize());
				splitNode.setSize(this.getMinSize());
				
			} else {
				
				// new Child is inserted into right Node
				
				System.arraycopy(parent.getKeys(), middle + 1, splitNode.getKeys(), 0, this.getMinSize() -1);
				System.arraycopy(parent.getChildren(), middle + 1, splitNode.getChildren(), 0, this.getMinSize());
				
				Arrays.fill(parent.getKeys(), middle + 1, this.getMaxSize() -1, 0);
				Arrays.fill(parent.getChildren(), middle + 1, this.getMaxSize(), -1);
				
				parent.setSize(this.getMinSize());
				splitNode.setSize(this.getMinSize() -1);
				
				insert(splitNodeID, key, newNodeID);
				
			}
			
			int middleKey = parent.getKey(middle);
			parent.getKeys()[middle] = 0;
			
			if (parentID == getRoot()) {
				return keyIDPair(splitNode.getKey(0), splitNodeID);
			}
			
			return propagateSplit(Arrays.copyOfRange(searchPath, 0, searchPath.length -2), splitNodeID, middleKey);

		} else { 
			
			/*
			 * 		 * parent has free space, adjust the key and ID to the first element in the new
			 * 	 * node	 */
			/* and its ID respectively TODO messup here */
			for (int i = 0; i < parentSize; ++i) {
				if (key < parentKeys[i]) {
					insertPosition = i;
					break;
				}
			}
			System.out.println(insertPosition + " key " + key);
			insert(parentID, this.getNode(newNodeID).getKey(0), -1);

			System.arraycopy(parentChildren, insertPosition, parentChildren, insertPosition + 1,
					parentSize - insertPosition);
			parent.setChildID(insertPosition, newNodeID);

			return NO_CHANGES; /* TERMINATION CONDITION */
		}
	}

	private long propagateMerge(int[] searchPath, int emptyNodeID) {
		// TODO implement this analog to Split
		throw new UnsupportedOperationException("Not yet implemented.");
	}
}

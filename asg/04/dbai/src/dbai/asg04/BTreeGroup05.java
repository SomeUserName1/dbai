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
        for (int leaf_key : mNode.getKeys()) if (key == leaf_key) return true;
        /* if we looped over all keys but didnt find anything, the key is not present */
        return false;
    }

    @Override
    protected long insertKey(final int nodeID, final int key) {
        int[] searchPath = searchLeaf(nodeID, key);
        int mNodeID = searchPath[searchPath.length - 1];

        Node mNode = this.getNode(mNodeID);
        int nodeSize = mNode.getSize();
        /* if the key is present skip the insertion according to the exercise description */
        if (this.containsKey(nodeID, key)) return NO_CHANGES;


        /* if there is space left ... */
        if (nodeSize < this.getMaxSize()) {
            /* and the key is larger than the current largest key in the node append the key */
            insert(mNodeID, key);
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
            System.arraycopy(splitKeys, this.getMinSize(), newNode.getKeys(), 0,
                    splitNodeSize);

            System.arraycopy(new int[splitNodeSize], 0, splitKeys, this.getMinSize(),
                    splitNodeSize);

            newNode.setSize(splitNodeSize);
            mNode.setSize(this.getMinSize());

            if (insertPosition < this.getMinSize()) {
                insert(mNodeID, key);
            } else {
                insert(newNodeID, key);
            }
            incrementSize();

            if (mNodeID == this.getRoot()) {
                return keyIDPair(newNode.getKey(0), newNodeID);
            }

            return propagateSplit(searchPath, newNodeID);
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

    private void insert(int nodeID, int key) {
        Node mNode = this.getNode(nodeID);
        int nodeSize = mNode.getSize();
        int[] leaf_keys = mNode.getKeys();

        if (nodeSize == 0) {
            mNode.setKey(nodeSize,key);
            mNode.setSize(nodeSize + 1);
        } else if (key > mNode.getKey(nodeSize-1)) {
            System.out.println(mNode.getKey(nodeSize-1));
            System.out.println(mNode.toString());
            mNode.setSize(nodeSize + 1);
            mNode.setKey(nodeSize, key);
        } else { /* else look for the next larger key and insert */
            for (int i = 0; i < nodeSize; ++i) {
                if (key < leaf_keys[i]) {
                    /* shift all other keys to the right */
                    System.arraycopy(leaf_keys, i, leaf_keys, i + 1, nodeSize - i);
                    /* insert the key */
                    mNode.setKey(i, key);
                    mNode.setSize(nodeSize + 1);
                    return;
                }
            }
        }
    }

    /**
     * @param nodeID the currently visited node
     * @param key    the key to be found
     * @return the search path including the leaf at position searchPath[searchPath.length-1]
     */
    private int[] searchLeaf(final int nodeID, final int key) {
        Node mNode = this.getNode(nodeID);
        if (mNode.isLeaf()) {
            /* if we reached here we selected the corresponding leaf and return its ID */
            return new int[]{nodeID};
        } else {
            /* if the node is not a leaf, ... */
            /*  check weather it is larger than the largest key of the current node.*/
            /* If so the location of the desired node must be in the subtree that the */
            /* last child is pointing to */

            /* else check the other keys and find the next larger one */
            int i = 0;
            for (int node_key : mNode.getKeys()) {
                /* check if key can be found in left children */
                if (key < node_key) {
                    return concat(new int[]{nodeID}, this.searchLeaf(mNode.getChildID(i), key));
                }
                i++;
            }
            return concat(new int[]{nodeID}, this.searchLeaf(mNode.getChildID(mNode.getSize()), key));
        }
    }


    /**
     * @param searchPath the path from the root to the according leaf or reduced path while recursion
     * @param newNodeID  the id of the child to be inserted
     * @return NO_CHANGE or the key-id pair of the new node and the middle key
     */
    private long propagateSplit(int[] searchPath, int newNodeID) {
        /* get parent node of the leaf. the leaf is at searchPath[searchPath.length-1], so the parent is at len-2 */
        int parentID = searchPath[searchPath.length - 2];
        Node parent = this.getNode(parentID);
        int parentSize = parent.getSize();
        int[] parentKeys = parent.getKeys();
        int[] parentChildren = parent.getChildren();
        int insertKey = this.getNode(newNodeID).getKey(0);
        int insertPosition = parentSize;


        /* if there is no free space left */
        if (!(parentSize < this.getMaxSize())) {
            /* find the mid key */
            /* if insertPosition doesn't change after the following for-loop it's the largest element */
            int extraChild = newNodeID;
            int midKey;
            int middle = (parentSize + 1) / 2;

            for (int i = 0; i < parentSize; ++i) {
                if (insertKey < parentKeys[i]) insertPosition = i;
            }
            System.out.println(insertPosition + " key " + insertKey);

            if (insertPosition == middle) {
                /* no shifting necessary as the new value is the mid key */
                midKey = insertKey;

            } else if (insertPosition < middle) {
                /* the place to insert the new key is below the mid of the actual array */
                /* so if one would insert the element in a new array the mid would be one to the left */
                midKey = parentKeys[middle - 1];

                /* shift the places after and including the insertion position one place towards the middle */
                System.arraycopy(parentKeys, insertPosition, parentKeys, insertPosition + 1,
                        middle - insertPosition);

                /* insert the new key */
                parentKeys[insertPosition] = insertKey;

            } else {
                /* the place to insert the new key is above the mid of the actual array so it's not affected */
                /* by the insertion */
                midKey = parentKeys[middle];

                /* shift the places before and including the insertion position one place towards the middle */
                System.arraycopy(parentKeys, middle + 1, parentKeys, middle, insertPosition - (middle + 1));
                parentKeys[insertPosition - 1] = insertKey;
            }

            /* if we are at the root return the keyIDPair to split the root node */
            if (parentID == this.getRoot())
                return keyIDPair(midKey, newNodeID); /* TERMINATION CONDITION*/

            /* if the to be inserted child was the biggest element continue as it wont fit into the existing children */
            /* array and extraChild was initialized to that. Else safe the one that'd get overwritten, shift all */
            /* children pointers but the last one place to the right and insert the new child accordingly */

            if (insertPosition < parentSize) {

                extraChild = parentChildren[parentSize];
                System.arraycopy(parentChildren, insertPosition + 1, parentChildren, insertPosition + 2,
                        parentSize - insertPosition);
                parentChildren[parentSize] = newNodeID;
            }

            /* do the split  */
            int splitNodeID = this.createNode(false);
            Node splitNode = this.getNode(splitNodeID);
            int splitNodeSize = parentSize - this.getMinSize();

            /* copy the upper part over for both keys and children and append the extra child to the children */
            System.arraycopy(parentChildren, this.getMinSize() + 1, splitNode.getChildren(), 0,
                    splitNodeSize);
            splitNode.setChildID(parent.getSize() - this.getMinSize(), extraChild);
            System.arraycopy(parentKeys, this.getMinSize(), splitNode.getKeys(), 0,
                    splitNodeSize);
            System.arraycopy(new int[splitNodeSize], 0, parentKeys, this.getMinSize(),
                    splitNodeSize);

            /* Adjust the sizes accordingly */
            splitNode.setSize(splitNodeSize);
            parent.setSize(this.getMinSize()); /* parentSize-(parentSize-this.getMinSize()) == this.getMinSize() */

            /* reduce search path and recurse */
            return propagateSplit(Arrays.copyOf(searchPath, searchPath.length - 1), splitNodeID);

        } else { /* parent has free space, adjust the key and ID to the first element in the new node */
            /* and its ID respectively TODO messup here*/
            for (int i = 0; i < parentSize; ++i) {
                if (insertKey < parentKeys[i]) insertPosition = i;
            }
            System.out.println(insertPosition + " key " + insertKey);
            insert(parentID, this.getNode(newNodeID).getKey(0));

            System.arraycopy(parentChildren, insertPosition + 1, parentChildren, insertPosition + 2,
                    parentSize - insertPosition);
            parent.setChildID(insertPosition + 1, newNodeID);


            return NO_CHANGES; /* TERMINATION CONDITION */
        }
    }

    private long propagateMerge(int[] searchPath, int emptyNodeID) {
        // TODO implement this analog to Split
        throw new UnsupportedOperationException("Not yet implemented.");
    }
}

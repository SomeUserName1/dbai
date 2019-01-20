/*
 * @(#)BTreeImpl.java   1.0   Nov 19, 2013
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.access.btree;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;

import minibase.RecordID;
import minibase.SearchKey;
import minibase.SearchKeyType;
import minibase.storage.buffer.BufferManager;
import minibase.storage.buffer.Page;
import minibase.storage.buffer.PageID;
import minibase.storage.buffer.UnpinMode;

/**
 * Implementations of the core algorithms of a B+ Tree index.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class BTreeImpl extends BTreeIndex {

   /**
    * Constructor for opening an existing index.
    *
    * @param bufferManager
    *           the buffer manager
    * @param name
    *           name of the index
    * @param header
    *           the index's header page
    * @param typeID
    *           the id of the key type
    */
   BTreeImpl(final BufferManager bufferManager, final Optional<String> name, final Page<BTreeHeader> header,
         final SearchKeyType typeID) {
      super(bufferManager, name, header, typeID);
   }

   @Override
   public void delete() {

      if (this.getFileName().isPresent()) {
         final PageID headerID = this.getBufferManager()
               .getDiskManager().getFileEntry(this.getFileName().get());
         if (headerID == null) {
            // index does not exist
            throw new IllegalArgumentException("There is no index with the name '"
               + this.getFileName() + "'.");
         }
      }

      // get the root page ID and delete the header page
      final Page<BTreeHeader> header = this.getHeader();
      final PageID rootID = BTreeHeader.getRootID(header);


      // delete the tree pages
      final ArrayDeque<PageID> stack = new ArrayDeque<>();
      stack.add(rootID);
      do {
         final PageID pageID = stack.pop();
         final Page<BTreePage> page = this.getBufferManager().pinPage(pageID);
         if (!BTreePage.isLeafPage(page)) {
            final Page<BTreeBranch> branch = BTreeBranch.cast(page);
            final int numChildren = BTreePage.getNumKeys(branch) + 1;
            for (int childPos = 0; childPos < numChildren; childPos++) {
               stack.push(BTreeBranch.getChildID(branch, childPos));
            }
         }
         this.getBufferManager().freePage(page);
      } while (!stack.isEmpty());

      // delete the logical file
      if (this.getFileName().isPresent()) {
         this.getBufferManager().getDiskManager().deleteFileEntry(this.getFileName().get());
      }
      this.getBufferManager().freePage(this.getHeader());
      this.removeHeader();
   }

   @Override
   protected int findKey(final Page<? extends BTreePage> page, final SearchKey key, final boolean leaf) {
      int leftIncl = 0;
      int rightExcl = BTreePage.getNumKeys(page);
      while (leftIncl < rightExcl) {
         final int mid = leftIncl + (rightExcl - leftIncl) / 2;
         final SearchKey currKey = leaf
               ? BTreeLeaf.getKey(BTreeLeaf.cast(page), mid, this.getKeyType())
               : BTreeBranch.getKey(BTreeBranch.cast(page), mid, this.getKeyType());
         if (currKey.compareTo(key) < 0) {
            leftIncl = mid + 1;
         } else if (currKey.compareTo(key) == 0) {
            return mid;
         } else {
            rightExcl = mid;
         }
      }
      return -(leftIncl + 1);
   }

   @Override
   protected Page<BTreeLeaf> search(final PageID rootID, final SearchKey key) {
      final BufferManager bufferManager = this.getBufferManager();
      // get page from disk
      PageID pageID = rootID;
      Page<BTreePage> page = bufferManager.pinPage(pageID);
      while (!BTreePage.isLeafPage(page)) {
         // continue with child page
         final Page<BTreeBranch> branch = BTreeBranch.cast(page);
         pageID = BTreeBranch.getChildID(branch, Math.abs(this.findKey(branch, key, false) + 1));
         bufferManager.unpinPage(page, UnpinMode.CLEAN);
         page = bufferManager.pinPage(pageID);
      }
      return BTreeLeaf.cast(page);
   }

   @Override
   protected Entry insert(final PageID rootID, final SearchKey key, final RecordID value) {
      return this.insert(rootID, key, value, null, 0, 0);
   }

   /**
    * Recursive helper method for {@link #insert(PageID, int, RecordID)}.
    * @param pageID ID of the page to insert into
    * @param key key to insert
    * @param value record ID to insert
    * @param parentID ID of the parent, {@code null} for the root page
    * @param parPos position of the current page's ID in the parent page
    * @param parNumKeys number of keys in the parent page
    * @return an entry consisting of the middle key and the ID of the child page to insert
    *         if the page was split as a result of the insertion, {@code null} otherwise
    */
   private Entry insert(final PageID pageID, final SearchKey key, final RecordID value,
         final PageID parentID, final int parPos, final int parNumKeys) {
      final BufferManager bufferManager = this.getBufferManager();
      // get page from disk
      final Page<BTreePage> page = bufferManager.pinPage(pageID);
      final int rawKeySize = this.getKeyType().getKeyLength();
      if (BTreePage.isLeafPage(page)) {
         final Page<BTreeLeaf> leaf = BTreeLeaf.check(page);
         return this.insertLeaf(pageID, leaf, key, value, parentID, parPos, parNumKeys);
      }

      // current page is an inner node -- find the next child
      final Page<BTreeBranch> branch = BTreeBranch.check(page);
      final int numKeys = BTreePage.getNumKeys(branch);
      final int pos = Math.abs(this.findKey(branch, key, false) + 1);

      // continue insertion in child page
      final PageID childID = BTreeBranch.getChildID(branch, pos);
      final Entry newChild = this.insert(childID, key, value, pageID, pos, numKeys);

      if (newChild == null) {
         // no structural change
         bufferManager.unpinPage(branch, UnpinMode.CLEAN);
         return null;
      }

      // some space is left - insert the new entry and return
      if (numKeys < this.getMaxBranchKeys()) {
         BTreeBranch.insertEntry(branch, pos, newChild.getMiddleKey(),
               newChild.getChildID(), this.getKeyType());
         bufferManager.unpinPage(branch, UnpinMode.DIRTY);
         return null;
      }

      // we try to shift one entry into a neighboring page first
      if (parPos > 0) {
         // try to shift one entry into the previous sibling page
         final Page<BTreeBranch> parent = bufferManager.pinPage(parentID);
         final PageID prevID = BTreeBranch.getChildID(parent, parPos - 1);
         final Page<BTreeBranch> prev = bufferManager.pinPage(prevID);
         final int prevNumKeys = BTreePage.getNumKeys(prev);
         final int move = (numKeys - prevNumKeys + 1) / 2;
         if (move > 0) {
            // the previous page gets entries
            if (pos < move - 1) {
               // the new entry goes into the left node
               rotateLeft(parent, parPos - 1, prev, branch, move - 1, this.getKeyType());
               BTreeBranch.insertEntry(prev, prevNumKeys + pos + 1,
                     newChild.getMiddleKey(), newChild.getChildID(), this.getKeyType());
            } else if (pos == move - 1) {
               // the new entry goes into the middle
               rotateLeft(parent, parPos - 1, prev, branch, move, this.getKeyType());
               final SearchKey midKey = BTreeBranch.getKey(parent, parPos - 1, this.getKeyType());
               BTreeBranch.setKey(parent, parPos - 1, newChild.getMiddleKey(), rawKeySize);
               BTreeBranch.insertEntry(branch, 0, midKey, BTreeBranch.getChildID(branch, 0),
                     this.getKeyType());
               BTreeBranch.setChildID(branch, 0, newChild.getChildID());
            } else {
               // the new entry goes into the right node
               rotateLeft(parent, parPos - 1, prev, branch, move, this.getKeyType());
               BTreeBranch.insertEntry(branch, pos - move, newChild.getMiddleKey(),
                     newChild.getChildID(), this.getKeyType());
            }
            BTreePage.setNumKeys(prev, prevNumKeys + move);
            BTreePage.setNumKeys(branch, numKeys - move + 1);

            bufferManager.unpinPage(prev, UnpinMode.DIRTY);
            bufferManager.unpinPage(parent, UnpinMode.DIRTY);
            bufferManager.unpinPage(branch, UnpinMode.DIRTY);
            return null;
         }
         bufferManager.unpinPage(prev, UnpinMode.CLEAN);
         bufferManager.unpinPage(parent, UnpinMode.CLEAN);
      }

      if (parPos < parNumKeys) {
         // try to shift entries into the next sibling page
         final Page<BTreeBranch> parent = bufferManager.pinPage(parentID);
         final PageID nextID = BTreeBranch.getChildID(parent, parPos + 1);
         final Page<BTreeBranch> next = bufferManager.pinPage(nextID);
         final int nextNumKeys = BTreePage.getNumKeys(next);
         final int move = (numKeys - nextNumKeys + 1) / 2;
         if (move > 0) {
            // there's space left, rotate values over
            final int newNumKeys = numKeys - move + 1;
            if (pos < newNumKeys) {
               // the new entry goes into the left node
               rotateRight(parent, parPos, branch, next, move, this.getKeyType());
               BTreeBranch.insertEntry(branch, pos, newChild.getMiddleKey(),
                     newChild.getChildID(), this.getKeyType());
            } else if (pos == newNumKeys) {
               // the new entry goes into the middle
               rotateRight(parent, parPos, branch, next, move - 1, this.getKeyType());
               final SearchKey midKey = BTreeBranch.getKey(parent, parPos, this.getKeyType());
               BTreeBranch.setKey(parent, parPos, newChild.getMiddleKey(), rawKeySize);
               BTreeBranch.insertEntry(next, 0, midKey,
                     BTreeBranch.getChildID(next, 0), this.getKeyType());
               BTreeBranch.setChildID(next, 0, newChild.getChildID());
            } else {
               // the new entry goes into the right node
               rotateRight(parent, parPos, branch, next, move - 1, this.getKeyType());
               BTreeBranch.insertEntry(next, pos - newNumKeys - 1,
                     newChild.getMiddleKey(), newChild.getChildID(), this.getKeyType());
            }
            BTreePage.setNumKeys(branch, newNumKeys);
            BTreePage.setNumKeys(next, nextNumKeys + move);

            bufferManager.unpinPage(next, UnpinMode.DIRTY);
            bufferManager.unpinPage(parent, UnpinMode.DIRTY);
            bufferManager.unpinPage(branch, UnpinMode.DIRTY);
            return null;
         }
         bufferManager.unpinPage(next, UnpinMode.CLEAN);
         bufferManager.unpinPage(parent, UnpinMode.CLEAN);
      }

      // we have to split the page
      final Page<BTreeBranch> left = branch;
      final Page<BTreeBranch> right = BTreeBranch.newPage(bufferManager);
      final PageID rightID = right.getPageID();

      // choose page in which the new child will be inserted
      final SearchKey midKey;
      final int minBranchKeys = this.getMinBranchKeys();
      if (pos < minBranchKeys) {
         // insert into left block
         midKey = BTreeBranch.getKey(left, minBranchKeys - 1, this.getKeyType());
         BTreeBranch.setChildID(right, 0, BTreeBranch.getChildID(left, minBranchKeys));
         BTreeBranch.copyEntries(left, minBranchKeys, right, 0, minBranchKeys, rawKeySize);
         BTreePage.setNumKeys(left, minBranchKeys - 1);
         BTreePage.setNumKeys(right, minBranchKeys);
         BTreeBranch.insertEntry(left, pos, newChild.getMiddleKey(),
               newChild.getChildID(), this.getKeyType());
      } else if (pos == minBranchKeys) {
         // insert in the middle, the key goes up one level
         midKey = newChild.getMiddleKey();
         BTreeBranch.setChildID(right, 0, newChild.getChildID());
         BTreeBranch.copyEntries(left, minBranchKeys, right, 0, minBranchKeys, rawKeySize);
         BTreePage.setNumKeys(left, minBranchKeys);
         BTreePage.setNumKeys(right, minBranchKeys);
      } else {
         // insert into right block
         midKey = BTreeBranch.getKey(left, minBranchKeys, this.getKeyType());
         BTreeBranch.setChildID(right, 0,
               BTreeBranch.getChildID(left, minBranchKeys + 1));
         BTreeBranch
               .copyEntries(left, minBranchKeys + 1, right, 0,
                     minBranchKeys - 1, rawKeySize);
         BTreePage.setNumKeys(left, minBranchKeys);
         BTreePage.setNumKeys(right, minBranchKeys - 1);
         BTreeBranch.insertEntry(right, pos - minBranchKeys - 1,
               newChild.getMiddleKey(), newChild.getChildID(), this.getKeyType());
      }

      // unpin pages and return new pointer
      bufferManager.unpinPage(branch, UnpinMode.DIRTY);
      bufferManager.unpinPage(right, UnpinMode.DIRTY);
      return new Entry(midKey, rightID);
   }

   /**
    * Inserts the given entry into the given leaf page.
    *
    * @param pageID
    *           leaf page's ID
    * @param page
    *           the page
    * @param key
    *           key to insert
    * @param value
    *           record ID to insert
    * @param parentID
    *           page ID of the parent page, {@link PageID#INVALID} for the root page
    * @param parPos
    *           position of this page's ID in the parent page
    * @param parNumKeys
    *           number of keys in the parent page
    * @return pair of middle key and new page ID if the page was split, {@code null}
    *         otherwise
    */
   private Entry insertLeaf(final PageID pageID, final Page<BTreeLeaf> page, final SearchKey key,
         final RecordID value, final PageID parentID, final int parPos,
         final int parNumKeys) {
      // check if the leaf value is found
      final BufferManager bufferManager = this.getBufferManager();
      final int numKeys = BTreePage.getNumKeys(page);
      final int found = this.findKey(page, key, true);
      final int rawKeySize = this.getKeyType().getKeyLength();
      if (found >= 0) {
         // key already exists
         BTreeLeaf.setRecordID(page, found, value, rawKeySize);
         bufferManager.unpinPage(page, UnpinMode.DIRTY);
         return null;
      }
      final int pos = -(found + 1);

      // key will be inserted, update index size
      this.incrementSize();

      if (numKeys < this.getMaxLeafKeys()) {
         // some space is left - insert the value and return
         BTreeLeaf.insertEntry(page, pos, key, value, rawKeySize);
         bufferManager.unpinPage(page, UnpinMode.DIRTY);
         return null;
      }

      // try to avoid splitting the page by filling a neighboring one
      final PageID nextID = BTreeLeaf.getNextPage(page);
      if (parPos < parNumKeys) {
         final Page<BTreeLeaf> next = bufferManager.pinPage(nextID);
         final int nextNumKeys = BTreePage.getNumKeys(next);
         final int move = (numKeys - nextNumKeys + 1) / 2;
         if (move > 0) {
            // push values to the right node
            final Page<BTreeBranch> parent = bufferManager.pinPage(parentID);
            final int newNumKeys = numKeys - move + 1;
            if (pos < newNumKeys) {
               rotateRight(parent, parPos, page, next, move, this.getKeyType());
               BTreeLeaf.insertEntry(page, pos, key, value, rawKeySize);
            } else if (pos == newNumKeys) {
               rotateRight(parent, parPos, page, next, move - 1, this.getKeyType());
               BTreeLeaf.insertEntry(next, 0, key, value, rawKeySize);
               BTreeBranch.setKey(parent, parPos, key, rawKeySize);
            } else {
               rotateRight(parent, parPos, page, next, move - 1, this.getKeyType());
               BTreeLeaf.insertEntry(next, pos - newNumKeys, key, value, rawKeySize);
            }
            bufferManager.unpinPage(parent, UnpinMode.DIRTY);
            bufferManager.unpinPage(next, UnpinMode.DIRTY);
            bufferManager.unpinPage(page, UnpinMode.DIRTY);
            return null;
         }
         bufferManager.unpinPage(next, UnpinMode.CLEAN);
      }

      final PageID prevID = BTreeLeaf.getPrevPage(page);
      if (parPos > 0) {
         final Page<BTreeLeaf> prev = bufferManager.pinPage(prevID);
         final int prevNumKeys = BTreePage.getNumKeys(prev);
         final int move = (numKeys - prevNumKeys + 1) / 2;
         if (move > 0) {
            // push values to the left node
            final Page<BTreeBranch> parent = bufferManager.pinPage(parentID);
            if (pos == 0 && move == 1) {
               // no rotation, entry goes to the left page
               BTreeLeaf.insertEntry(prev, prevNumKeys, key, value, rawKeySize);
               BTreeBranch.setKey(parent, parPos - 1, BTreeLeaf.getKey(page, 0, this.getKeyType()),
                     rawKeySize);
               bufferManager.unpinPage(page, UnpinMode.CLEAN);
            } else {
               if (pos < move) {
                  rotateLeft(parent, parPos - 1, prev, page, move - 1, this.getKeyType());
                  BTreeLeaf.insertEntry(prev, prevNumKeys + pos, key, value, rawKeySize);
               } else if (pos == move) {
                  rotateLeft(parent, parPos - 1, prev, page, move, this.getKeyType());
                  BTreeLeaf.insertEntry(page, 0, key, value, rawKeySize);
                  BTreeBranch.setKey(parent, parPos - 1, key, rawKeySize);
               } else {
                  rotateLeft(parent, parPos - 1, prev, page, move, this.getKeyType());
                  BTreeLeaf.insertEntry(page, pos - move, key, value, rawKeySize);
               }
               bufferManager.unpinPage(page, UnpinMode.DIRTY);
            }
            bufferManager.unpinPage(parent, UnpinMode.DIRTY);
            bufferManager.unpinPage(prev, UnpinMode.DIRTY);
            return null;
         }
         bufferManager.unpinPage(prev, UnpinMode.CLEAN);
      }

      // no space is left - allocate new page and distribute entries
      final Page<BTreeLeaf> right = BTreeLeaf.newPage(bufferManager, pageID, nextID);
      final PageID rightID = right.getPageID();
      BTreeLeaf.setNextPage(page, rightID);

      if (nextID.isValid()) {
         // update the next page's previous-page pointer
         final Page<BTreeLeaf> next = bufferManager.pinPage(nextID);
         BTreeLeaf.setPrevPage(next, rightID);
         bufferManager.unpinPage(next, UnpinMode.DIRTY);
      }

      // distribute the entries
      BTreeLeaf.copyEntries(page, this.getMinLeafKeys(), right, 0, this.getMinLeafKeys(), rawKeySize);
      BTreePage.setNumKeys(page, this.getMinLeafKeys());
      BTreePage.setNumKeys(right, this.getMinLeafKeys());

      // choose page in which the new entry will be inserted
      if (pos < this.getMinLeafKeys()) {
         BTreeLeaf.insertEntry(page, pos, key, value, rawKeySize);
      } else {
         BTreeLeaf.insertEntry(right, pos - this.getMinLeafKeys(), key, value, rawKeySize);
      }

      final SearchKey midKey = BTreeLeaf.getKey(right, 0, this.getKeyType());
      bufferManager.unpinPage(page, UnpinMode.DIRTY);
      bufferManager.unpinPage(right, UnpinMode.DIRTY);
      return new Entry(midKey, rightID);
   }

   @Override
   public boolean remove(final SearchKey key, final RecordID recordID) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(recordID);
      final PageID rootID = BTreeHeader.getRootID(this.getHeader());
      return this.delete(key, recordID, rootID) != DeleteResponse.FAILURE;
   }

   @Override
   protected DeleteResponse delete(final SearchKey key, final RecordID rid, final PageID pageID) {
      // get page from disk
      final BufferManager bufferManager = this.getBufferManager();
      final Page<BTreePage> page = bufferManager.pinPage(pageID);
      final int numKeys = BTreePage.getNumKeys(page);
      final int rawKeySize = this.getKeyType().getKeyLength();
      // check if the current node is a leaf node
      if (BTreePage.isLeafPage(page)) {
         final Page<BTreeLeaf> leaf = BTreeLeaf.cast(page);
         final int pos = this.findKey(leaf, key, true);

         if (pos < 0) {
            // key not found, nothing to do
            bufferManager.unpinPage(leaf, UnpinMode.CLEAN);
            return DeleteResponse.FAILURE;
         }

         // key was found
         BTreeLeaf.deleteEntry(leaf, pos, rawKeySize);

         // update index size
         this.decrementSize();

         bufferManager.unpinPage(leaf, UnpinMode.DIRTY);
         // check if page is still full enough
         if (numKeys > this.getMinLeafKeys()) {
            return DeleteResponse.NOMERGE;
         } else {
            return DeleteResponse.MERGE;
         }
      }

      // current page is no leaf - find the next child page
      final Page<BTreeBranch> branch = BTreeBranch.cast(page);
      final int pos = Math.abs(this.findKey(branch, key, false) + 1);
      final PageID childID = BTreeBranch.getChildID(branch, pos);

      // return when value was not found or everything is done
      final DeleteResponse response = this.delete(key, rid, childID);
      if (response != DeleteResponse.MERGE) {
         bufferManager.unpinPage(branch, UnpinMode.CLEAN);
         return response;
      }

      // child is under-full
      final Page<BTreePage> child = bufferManager.pinPage(childID);
      final int childSize = BTreePage.getNumKeys(child);

      if (pos > 0) {
         // try to steal entries from the left
         final PageID leftID = BTreeBranch.getChildID(branch, pos - 1);
         final Page<BTreePage> left = bufferManager.pinPage(leftID);
         final int leftSize = BTreePage.getNumKeys(left);
         final int move = (leftSize - childSize) / 2;
         if (move > 0) {
            rotateRight(branch, pos - 1, left, child, move, this.getKeyType());
            bufferManager.unpinPage(left, UnpinMode.DIRTY);
            bufferManager.unpinPage(child, UnpinMode.DIRTY);
            bufferManager.unpinPage(branch, UnpinMode.DIRTY);
            return DeleteResponse.NOMERGE;
         }
         bufferManager.unpinPage(left, UnpinMode.CLEAN);
      }

      if (pos < numKeys) {
         // try to steal entries from the right
         final PageID rightID = BTreeBranch.getChildID(branch, pos + 1);
         final Page<BTreeBranch> right = bufferManager.pinPage(rightID);
         final int rightSize = BTreePage.getNumKeys(right);
         final int move = (rightSize - childSize) / 2;
         if (move > 0) {
            rotateLeft(branch, pos, child, right, move, this.getKeyType());
            bufferManager.unpinPage(right, UnpinMode.DIRTY);
            bufferManager.unpinPage(child, UnpinMode.DIRTY);
            bufferManager.unpinPage(branch, UnpinMode.DIRTY);
            return DeleteResponse.NOMERGE;
         }
         bufferManager.unpinPage(right, UnpinMode.CLEAN);
      }

      // merge with a neighbor
      final int leftPos = pos > 0 ? pos - 1 : pos;
      bufferManager.unpinPage(child, UnpinMode.CLEAN);
      this.mergeChildren(branch, leftPos);

      if (numKeys == 1) {
         // we're the root node and there's no more sibling,
         // make the only child node the root
         this.setRootID(BTreeBranch.getChildID(branch, leftPos));
         bufferManager.freePage(branch);
         return DeleteResponse.NOMERGE;
      }

      BTreeBranch.deleteEntry(branch, leftPos, rawKeySize);
      bufferManager.unpinPage(branch, UnpinMode.DIRTY);
      if (numKeys > this.getMinBranchKeys()) {
         return DeleteResponse.NOMERGE;
      } else {
         return DeleteResponse.MERGE;
      }
   }

   /**
    * Merges two children of the given page, namely those at positions {@code leftPos} and
    * {@code leftPos + 1}. The right one is deleted after that.
    *
    * @param page
    *           parent page
    * @param leftPos
    *           position of the left child to be merged
    */
   private void mergeChildren(final Page<BTreeBranch> page, final int leftPos) {
      final BufferManager bufferManager = this.getBufferManager();
      final Page<BTreePage> left = bufferManager.pinPage(BTreeBranch.getChildID(page, leftPos));
      final Page<BTreePage> right = bufferManager.pinPage(BTreeBranch.getChildID(page, leftPos + 1));
      final int leftSize = BTreePage.getNumKeys(left);
      final int rightSize = BTreePage.getNumKeys(right);
      final int rawKeySize = this.getKeyType().getKeyLength();
      if (BTreePage.isLeafPage(left)) {
         // just concatenate the entries
         final Page<BTreeLeaf> lLeaf = BTreeLeaf.cast(left);
         final Page<BTreeLeaf> rLeaf = BTreeLeaf.cast(right);
         BTreeLeaf.copyEntries(rLeaf, 0, lLeaf, leftSize, rightSize, rawKeySize);
         BTreePage.setNumKeys(lLeaf, leftSize + rightSize);

         // fix the previous- and next-pointers
         final PageID nextID = BTreeLeaf.getNextPage(rLeaf);
         BTreeLeaf.setNextPage(lLeaf, nextID);
         if (nextID.isValid()) {
            final Page<BTreeLeaf> next = bufferManager.pinPage(nextID);
            BTreeLeaf.setPrevPage(next, lLeaf.getPageID());
            bufferManager.unpinPage(next, UnpinMode.DIRTY);
         }
      } else {
         // we have to stick the middle value from the parent back in
         final Page<BTreeBranch> lBranch = BTreeBranch.cast(left);
         final Page<BTreeBranch> rBranch = BTreeBranch.cast(right);
         final SearchKey midKey = BTreeBranch.getKey(page, leftPos, this.getKeyType());
         final PageID firstID = BTreeBranch.getChildID(rBranch, 0);
         BTreeBranch.insertEntry(lBranch, leftSize, midKey, firstID, this.getKeyType());
         BTreeBranch.copyEntries(rBranch, 0, lBranch, leftSize + 1, rightSize, rawKeySize);
         BTreePage.setNumKeys(lBranch, leftSize + 1 + rightSize);
      }

      bufferManager.unpinPage(left, UnpinMode.DIRTY);
      bufferManager.freePage(right);
   }

   /**
    * Rotates {@code n} entries from page {@code right} over {@code page} to {@code left},
    * where {@code leftPos} is the position of the reference to {@code left} in the parent
    * page {@code page}. After this operation, {@code left} has {@code n} entries more and
    * {@code right} less than before.
    *
    * @param page
    *           parent page
    * @param leftPos
    *           position of the child page ID for {@code left} in {@code page}
    * @param left
    *           left child page
    * @param right
    *           right child page
    * @param n
    *           number of entries to shift over
    * @param keyType
    *           type of the key in byte
    */
   private static void rotateLeft(final Page<BTreeBranch> page, final int leftPos,
         final Page<? extends BTreePage> left, final Page<? extends BTreePage> right,
         final int n, final SearchKeyType keyType) {
      final int keySize = keyType.getKeyLength();
      if (n > 0) {
         final int leftSize = BTreePage.getNumKeys(left);
         final int rightSize = BTreePage.getNumKeys(right);
         final SearchKey newMidKey;
         if (BTreePage.isLeafPage(left)) {
            final Page<BTreeLeaf> lLeaf = BTreeLeaf.cast(left);
            final Page<BTreeLeaf> rLeaf = BTreeLeaf.cast(right);
            newMidKey = BTreeLeaf.getKey(rLeaf, n, keyType);
            BTreeLeaf.copyEntries(rLeaf, 0, lLeaf, leftSize, n, keySize);
            BTreeLeaf.shiftEntries(rLeaf, n, 0, rightSize - n, keySize);
         } else {
            final Page<BTreeBranch> lBranch = BTreeBranch.cast(left);
            final Page<BTreeBranch> rBranch = BTreeBranch.cast(right);
            newMidKey = BTreeBranch.getKey(rBranch, n - 1, keyType);
            BTreeBranch.insertEntry(lBranch, leftSize, BTreeBranch.getKey(page, leftPos, keyType),
                  BTreeBranch.getChildID(rBranch, 0), keyType);
            BTreeBranch.copyEntries(rBranch, 0, lBranch, leftSize + 1, n - 1, keySize);
            BTreeBranch.setChildID(rBranch, 0, BTreeBranch.getChildID(rBranch, n));
            BTreeBranch.shiftEntries(rBranch, n, 0, rightSize - n, keySize);
         }
         BTreeBranch.setKey(page, leftPos, newMidKey, keySize);
         BTreePage.setNumKeys(left, leftSize + n);
         BTreePage.setNumKeys(right, rightSize - n);
      }
   }

   /**
    * Rotates {@code n} entries from page {@code left} over {@code page} to {@code right},
    * where {@code leftPos} is the position of the reference to {@code left} in the parent
    * page {@code page}. After this operation, {@code right} has {@code n} entries more and
    * {@code left} less than before.
    *
    * @param page
    *           parent page
    * @param leftPos
    *           position of the child page ID for {@code left} in {@code page}
    * @param left
    *           left child page
    * @param right
    *           right child page
    * @param n
    *           number of entries to shift over
    * @param keyType
    *           type of the key in byte
    */
   private static void rotateRight(final Page<BTreeBranch> page, final int leftPos,
         final Page<? extends BTreePage> left, final Page<? extends BTreePage> right,
         final int n, final SearchKeyType keyType) {
      final int keySize = keyType.getKeyLength();
      if (n > 0) {
         final int leftSize = BTreePage.getNumKeys(left);
         final int rightSize = BTreePage.getNumKeys(right);
         final SearchKey midKey = BTreeBranch.getKey(page, leftPos, keyType);
         final SearchKey newMidKey;
         if (BTreePage.isLeafPage(left)) {
            final Page<BTreeLeaf> lLeaf = BTreeLeaf.cast(left);
            final Page<BTreeLeaf> rLeaf = BTreeLeaf.cast(right);
            newMidKey = BTreeLeaf.getKey(lLeaf, leftSize - n, keyType);
            BTreeLeaf.shiftEntries(rLeaf, 0, n, rightSize, keySize);
            BTreeLeaf.copyEntries(lLeaf, leftSize - n, rLeaf, 0, n, keySize);
         } else {
            final Page<BTreeBranch> lBranch = BTreeBranch.cast(left);
            final Page<BTreeBranch> rBranch = BTreeBranch.cast(right);
            newMidKey = BTreeBranch.getKey(lBranch, leftSize - n, keyType);
            BTreeBranch.shiftEntries(rBranch, 0, n, rightSize, keySize);
            BTreeBranch.setKey(rBranch, n - 1, midKey, keySize);
            BTreeBranch.setChildID(rBranch, n, BTreeBranch.getChildID(rBranch, 0));
            BTreeBranch.copyEntries(lBranch, leftSize - n + 1, rBranch, 0, n - 1, keySize);
            BTreeBranch.setChildID(rBranch, 0,
                  BTreeBranch.getChildID(lBranch, leftSize - n + 1));
         }
         BTreeBranch.setKey(page, leftPos, newMidKey, keySize);
         BTreePage.setNumKeys(left, leftSize - n);
         BTreePage.setNumKeys(right, rightSize + n);
      }
   }

   /**
    * Enum for the different responses of a delete action in the tree.
    *
    *
    * @author Raffael Wagner
    * @version 1.0
    */
   enum DeleteResponse {
      /** The deletion was unsuccessful. */
      FAILURE,
      /** The deletion was successful and the node is still full enough. */
      NOMERGE,
      /** The tree node is too empty. It has to be merged. */
      MERGE;
   }
}

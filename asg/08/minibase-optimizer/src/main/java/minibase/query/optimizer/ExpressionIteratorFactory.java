/*
 * @(#)ExpressionIteratorFactory.java   1.0   May 11, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Stack;

/**
 * Iterator that returns the sub-expressions of an expression in order, pre-order, or post-order. In order
 * traversal is only supported if <em>all</em> sub-expressions are unary or binary expressions.
 * <p>
 * Minibase's query optimizer is based on the Cascades framework for query optimization and, additionally,
 * implements some of the improvements proposed by the Columbia database query optimizer.
 * <ul>
 * <li>Goetz Graefe: <strong>The Cascades Framework for Query Optimization</strong>. In
 * <em>IEEE Data(base) Engineering Bulletin</em>, 18(3), pp. 19-29, 1995.</li>
 * <li>Yongwen Xu: <strong>Efficiency in Columbia Database Query Optimizer</strong>,
 * <em>MSc Thesis, Portland State University</em>, 1998.</li>
 * </ul>
 * The Minibase query optimizer therefore descends from the EXODUS, Volcano, Cascades, and Columbia line of
 * query optimizers, which all use a rule-based, top-down approach to explore the space of possible query
 * execution plans, rather than a bottom-up approach based on dynamic programming.
 * </p>
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class ExpressionIteratorFactory {

   /** Factory instance to access private classes. */
   private static final ExpressionIteratorFactory FACTORY = new ExpressionIteratorFactory();

   /**
    * Hidden constructor.
    */
   private ExpressionIteratorFactory() {
      // hidden constructor
   }

   /**
    * Returns a new expression iterator for the given root expression that returns sub-expressions with the
    * given traversal mode.
    *
    * @param root
    *           root expression
    * @param traversal
    *           traversal mode
    * @return expression iterator
    */
   public static Iterator<Expression> getIterator(final Expression root, final Traversal traversal) {
      switch (traversal) {
         case PREORDER:
            return FACTORY.new PreOrderExpressionIterator(root);
         case INORDER:
            return FACTORY.new InOrderExpressionIterator(root);
         case POSTORDER:
            return FACTORY.new PostOrderExpressionIterator(root);
         default:
            throw new IllegalStateException("Traversal mode " + traversal + " unknown.");
      }
   }

   /**
    * Defines the order types supported by this iterator.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   public enum Traversal {
      /** Pre-order traversal. */
      PREORDER,
      /** In order traversal. */
      INORDER,
      /** Post-order traversal. */
      POSTORDER;
   }

   /**
    * Expression iterator implementation that returns expressions in pre-order.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private final class PreOrderExpressionIterator implements Iterator<Expression> {

      /** Expression stack for non-recursive traversal. */
      private final Stack<Expression> stack;

      /**
       * Constructs an iterator that traverses the given expression in pre-order.
       *
       * @param root
       *           root expression
       */
      private PreOrderExpressionIterator(final Expression root) {
         this.stack = new Stack<>();
         this.stack.push(root);
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean hasNext() {
         return !this.stack.isEmpty();
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public Expression next() {
         if (this.hasNext()) {
            final Expression expression = this.stack.pop();
            this.pushChildren(this.stack, expression);
            return expression;
         }
         throw new NoSuchElementException();
      }

      /**
       * Pushes all children of the given parent expression onto the given stack in right to left order.
       *
       * @param stack
       *           stack to push child expressions onto
       * @param parent
       *           parent expression of child expressions
       */
      void pushChildren(final Stack<Expression> stack, final Expression parent) {
         for (int i = parent.getSize(); i > 0; i--) {
            this.stack.push(parent.getInput(i - 1));
         }
      }
   }

   /**
    * Abstract super class of expression iterator that use a queue to store the traversal as an intermediate
    * result.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private abstract class QueueBasedExpressionIterator implements Iterator<Expression> {

      /** Queue to store in order traversal. */
      private final Queue<Expression> queue;

      /**
       * Constructs a new iterator that uses a queue, which is initialized by a recursive traversal of the
       * given root expression.
       *
       * @param root
       *           root expression
       */
      private QueueBasedExpressionIterator(final Expression root) {
         this.queue = new LinkedList<>();
         this.traverse(root, this.queue);
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean hasNext() {
         return !this.queue.isEmpty();
      }

      @Override
      public Expression next() {
         while (this.hasNext()) {
            return this.queue.poll();
         }
         throw new NoSuchElementException();
      }

      /**
       * Recursive traversal of the given root expression that initializes the given queue.
       *
       * @param root
       *           root expression
       * @param queue
       *           result queue
       */
      abstract void traverse(Expression root, Queue<Expression> queue);
   }

   /**
    * Expression iterator implementation that returns expressions in order.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private final class InOrderExpressionIterator extends QueueBasedExpressionIterator {

      /**
       * Constructs an iterator that traverses the given expression in order.
       *
       * @param root
       *           root expression
       */
      InOrderExpressionIterator(final Expression root) {
         super(root);
      }

      /**
       * {@inheritDoc}
       */
      @Override
      void traverse(final Expression root, final Queue<Expression> queue) {
         if (root.getSize() > 2) {
            throw new IllegalStateException("In order unsupported for arity " + root.getSize() + " > 2.");
         }
         if (root.getSize() > 0) {
            this.traverse(root.getInput(0), queue);
         }
         queue.offer(root);
         if (root.getSize() > 1) {
            this.traverse(root.getInput(1), queue);
         }
      }
   }

   /**
    * Expression iterator implementation that returns expressions in post-order.
    *
    * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
    * @version 1.0
    */
   private final class PostOrderExpressionIterator extends QueueBasedExpressionIterator {

      /**
       * Constructs an iterator that traverses the given expression in post-order.
       *
       * @param root
       *           root expression
       */
      private PostOrderExpressionIterator(final Expression root) {
         super(root);
      }

      /**
       * {@inheritDoc}
       */
      @Override
      void traverse(final Expression root, final Queue<Expression> queue) {
         for (int i = 0; i < root.getSize(); i++) {
            this.traverse(root.getInput(i), queue);
         }
         queue.offer(root);
      }
   }
}

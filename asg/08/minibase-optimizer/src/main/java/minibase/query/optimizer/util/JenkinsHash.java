/*
 * @(#)JenkinsHash.java   1.0   Jan 5, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.util;

/**
 * A collection of hash functions defined by Bob Jenkins. These (non-cryptographic) hash
 * functions are fast and exhibit the avalanche effect property, which makes them useful as
 * incremental hash functions. These hash functions are intended for hash table lookups or
 * anything where one collision in 2<sup>32</sup> is acceptable. Use for cryptographic
 * purposes is not intended.
 * <p>
 * See <a
 * href="http://burtleburtle.net/bob/hash/evahash.html">http://burtleburtle.net/bob/hash
 * /evahash.html</a>
 * </p>
 * <p>
 * Minibase's query optimizer is based on the Cascades framework for query optimization and,
 * additionally, implements some of the improvements proposed by the Columbia database query
 * optimizer.
 * <ul>
 * <li>Goetz Graefe: <strong>The Cascades Framework for Query Optimization</strong>. In
 * <em>IEEE Data(base) Engineering Bulletin</em>, 18(3), pp. 19-29, 1995.</li>
 * <li>Yongwen Xu: <strong>Efficiency in Columbia Database Query Optimizer</strong>,
 * <em>MSc Thesis, Portland State University</em>, 1998.</li>
 * </ul>
 * The Minibase query optimizer therefore descends from the EXODUS, Volcano, Cascades, and
 * Columbia line of query optimizers, which all use a rule-based, top-down approach to
 * explore the space of possible query execution plans, rather than a bottom-up approach
 * based on dynamic programming.
 * </p>
 *
 * @author Bob Jenkins &lt;bob_jenkins@burtleburtle.net&gt;
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public final class JenkinsHash {

   /** 32-bit bit-mask to filter the integer part from a long. */
   private static final long INT_BITMASK = 0xFFFFFFFFL;

   /** 8-bit bit-mask to filter the byte part from a long. */
   private static final long BYTE_BITMASK = 0xFFL;

   /** Constant for value 'a' in mix(a,b,c). */
   private static final int A = 0;

   /** Constant for value 'b' in mix(a,b,c). */
   private static final int B = 1;

   /** Constant for value 'c' in mix(a,b,c). */
   private static final int C = 2;

   /**
    * Hashes a variable-length key into a 32-bit integer value. Note that negative values
    * will occur if the hash value is interpreted as a Java (signed) integer. Every bit of
    * the key affects every bit of the hash value. Every 1-bit and 2-bit delta achieves the
    * avalanche effect. The best hash table sizes are powers of two. There is no need to do
    * {@code mod} a prime. If less than 32 bits are required, a bit mask can be used.
    * The method implements Bob Jenkins' {@code lookup2} hash function that was
    * originally published in the Dr. Dobb's journal in 1997. The original C source code is
    * available on Bob Jenkins' <a href="http://www.burtleburtle.net/bob/c/lookup2.c">web
    * site</a>.
    *
    * @param key
    *           key (integer value will be treated as unsigned)
    * @param initialValue
    *           previous hash or an arbitrary value
    * @return hash value (32-bit integer value)
    */
   public static int lookup2(final int key, final int initialValue) {
      return lookup2(new byte[] { (byte) (key >>> 24), (byte) (key >>> 16),
            (byte) (key >>> 8), (byte) key }, initialValue);
   }

   /**
    * Hashes a variable-length key into a 32-bit integer value. Note that negative values
    * will occur if the hash value is interpreted as a Java (signed) integer. Every bit of
    * the key affects every bit of the hash value. Every 1-bit and 2-bit delta achieves the
    * avalanche effect. The best hash table sizes are powers of two. There is no need to do
    * {@code mod} a prime. If less than 32 bits are required, a bit mask can be used.
    * The method implements Bob Jenkins' {@code lookup2} hash function that was
    * originally published in the Dr. Dobb's journal in 1997. The original C source code is
    * available on Bob Jenkins' <a href="http://www.burtleburtle.net/bob/c/lookup2.c">web
    * site</a>.
    *
    * @param key
    *           key (unaligned array of bytes)
    * @param initialValue
    *           previous hash or an arbitrary value
    * @return hash value (32-bit integer value)
    */
   public static int lookup2(final byte[] key, final int initialValue) {
      // set up the internal state
      final int[] abc = new int[3];
      // the golden ratio, an arbitrary value
      abc[A] = 0x9e3779b9;
      abc[B] = abc[A];
      // the previous key
      abc[C] = initialValue;
      int length = key.length;
      int offset = 0;
      // handle most of the key
      while (length >= 12) {
         abc[A] = readInteger(key, offset);
         offset += 4;
         abc[B] = readInteger(key, offset);
         offset += 4;
         abc[C] = readInteger(key, offset);
         offset += 4;
         length -= 12;
         mix(abc);
      }
      // handle the last 11 bytes
      abc[C] = add(abc[C], length);
      // all the case statements fall through
      switch (length) {
         case 11:
            abc[C] = add(abc[C], (int) shiftLeft(key[offset + 10], 24));
         case 10:
            abc[C] = add(abc[C], (int) shiftLeft(key[offset + 9], 16));
         case 9:
            abc[C] = add(abc[C], (int) shiftLeft(key[offset + 8], 8));
         case 8:
            abc[B] = add(abc[B], (int) shiftLeft(key[offset + 7], 24));
         case 7:
            abc[B] = add(abc[B], (int) shiftLeft(key[offset + 6], 16));
         case 6:
            abc[B] = add(abc[B], (int) shiftLeft(key[offset + 5], 8));
         case 5:
            abc[B] = add(abc[B], (int) (key[offset + 4] & BYTE_BITMASK));
         case 4:
            abc[A] = add(abc[A], (int) shiftLeft(key[offset + 3], 24));
         case 3:
            abc[A] = add(abc[A], (int) shiftLeft(key[offset + 2], 16));
         case 2:
            abc[A] = add(abc[A], (int) shiftLeft(key[offset + 1], 8));
         case 1:
            abc[A] = add(abc[A], (int) (key[offset] & BYTE_BITMASK));
         case 0:
            // nothing left to add
         default:
            // cannot occur
      }
      mix(abc);
      // report the result
      return (int) (abc[C] & INT_BITMASK);
   }

   /**
    * Unsigned integer addition, simulated by using {@code long} arithmetic.
    *
    * @param augend
    *           augend
    * @param summand
    *           summand
    * @return sum of augend and summand
    */
   private static int add(final int augend, final int summand) {
      return (int) ((augend & INT_BITMASK) + (summand & INT_BITMASK));
   }

   /**
    * Unsigned integer subtraction, simulated by using {@code long} arithmetic.
    *
    * @param minuend
    *           minuend
    * @param subtrahend
    *           subtrahend
    * @return difference between minuend and subtrahend
    */
   private static int subtract(final int minuend, final int subtrahend) {
      return (int) ((minuend & INT_BITMASK) - (subtrahend & INT_BITMASK));
   }

   /**
    * Reads four bytes from the given buffer at the given offset and converts them to a
    * (signed) integer.
    *
    * @param buffer
    *           buffer to read four bytes from
    * @param offset
    *           offset to start reading at
    * @return integer integer converted from these four bytes
    */
   private static int readInteger(final byte[] buffer, final int offset) {
      return (int) ((buffer[offset] & BYTE_BITMASK) + shiftLeft(buffer[offset + 1], 8)
            + shiftLeft(buffer[offset + 2], 16) + shiftLeft(buffer[offset + 3], 24));
   }

   /**
    * Shifts the given byte value {@code num} positions to the left and returns the
    * result as a long.
    *
    * @param b
    *           byte value
    * @param num
    *           number of positions
    * @return shifted value
    */
   private static long shiftLeft(final byte b, final int num) {
      return (b & BYTE_BITMASK) << num;
   }

   /**
    * Mixes three integer values reversibly.
    *
    * @param abc
    *           integer array containing the three values, a, b, and c
    */
   private static void mix(final int[] abc) {
      abc[A] = subtract(abc[A], abc[B]);
      abc[A] = subtract(abc[A], abc[C]);
      abc[A] ^= abc[C] >>> 13;
      abc[B] = subtract(abc[B], abc[C]);
      abc[B] = subtract(abc[B], abc[A]);
      abc[B] ^= abc[A] << 9;
      abc[C] = subtract(abc[C], abc[A]);
      abc[C] = subtract(abc[C], abc[B]);
      abc[C] ^= abc[B] >>> 13;
      abc[A] = subtract(abc[A], abc[B]);
      abc[A] = subtract(abc[A], abc[C]);
      abc[A] ^= abc[C] >>> 12;
      abc[B] = subtract(abc[B], abc[C]);
      abc[B] = subtract(abc[B], abc[A]);
      abc[B] ^= abc[A] << 16;
      abc[C] = subtract(abc[C], abc[A]);
      abc[C] = subtract(abc[C], abc[B]);
      abc[C] ^= abc[B] >>> 5;
      abc[A] = subtract(abc[A], abc[B]);
      abc[A] = subtract(abc[A], abc[C]);
      abc[A] ^= abc[C] >>> 3;
      abc[B] = subtract(abc[B], abc[C]);
      abc[B] = subtract(abc[B], abc[A]);
      abc[B] ^= abc[A] << 10;
      abc[C] = subtract(abc[C], abc[A]);
      abc[C] = subtract(abc[C], abc[B]);
      abc[C] ^= abc[B] >>> 15;
   }

   /**
    * Hidden constructor.
    */
   private JenkinsHash() {
      // hidden constructor
   }
}

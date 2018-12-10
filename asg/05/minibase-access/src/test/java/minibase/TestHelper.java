/*
 * @(#)TestHelper.java   1.0   Aug 19, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase;

import org.junit.Assert;

/**
 * A test helper to help with testing.
 *
 * @author "Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt"
 * @version 1.0
 */
public final class TestHelper {

   /**
    * Hidden.
    */
   private TestHelper() {
      // no-op
   }

   /**
    * Asserts that the given block throws the given exception.
    *
    * @param exceptionClass
    *           the expected exception
    * @param block
    *           the block to run
    * @param <X> the type of the Exception
    * @return the expected exception or {@code null} if the exception was not thrown
    */
   public static <X extends Throwable> Throwable assertThrows(final Class<X> exceptionClass,
         final Runnable block) {
      try {
         block.run();
      } catch (final Throwable ex) {
         if (exceptionClass.isInstance(ex)) {
            return ex;
         }
      }
      Assert.fail("Failed to throw expected exception. Expected " + exceptionClass.getSimpleName());
      return null;
   }
}

/*
 * @(#)QueryException.java   1.0   Aug 2, 2006
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query;

/**
 * General exception for query execution issues.
 *
 * @author Chris Mayfield &lt;mayfiecs@jmu.edu&gt;
 * @author Manuel Hotz &lt;manuel.hotz@uni-konstanz.de&gt;
 * @since 1.0
 */
@SuppressWarnings("serial")
public class QueryException extends Exception {

   /**
    * Constructs a query exception, given the error message.
    *
    * @param message
    *           the message associated with this exception
    */
   public QueryException(final String message) {
      super(message);
   }

   /**
    * Constructs a query exception, given a cause.
    * 
    * @param cause
    *           cause for the exception
    */
   public QueryException(final Throwable cause) {
      super(cause);
   }

   /**
    * Constructs a query exception, given the error message and an underlying cause.
    * @param message message associated with the exception
    * @param cause cause for the query exception
    */
   public QueryException(final String message, final Throwable cause) {
      super(message, cause);
   }
}

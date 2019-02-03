/*
 * @(#)AbstractReference.java   1.0   Feb 14, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;

/**
 * Common abstract superclass of all reference implementations.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public abstract class AbstractReference implements Reference {

   /** Reference ID. */
   private final int id;

   /**
    * Constructs a new reference with the given identifier and name.
    *
    * @param id
    *           ID of this reference
    */
   AbstractReference(final int id) {
      this.id = id;
   }

   @Override
   public int getID() {
      return this.id;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + this.id;
      return result;
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (this.getClass() != obj.getClass()) {
         return false;
      }
      final AbstractReference other = (AbstractReference) obj;
      if (this.id != other.id) {
         return false;
      }
      return true;
   }
}

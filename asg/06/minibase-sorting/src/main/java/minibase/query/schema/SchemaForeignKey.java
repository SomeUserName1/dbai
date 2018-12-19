/*
 * @(#)SchemaForeignKey.java   1.0   Mar 4, 2015
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.schema;


import minibase.AbstractForeignKey;
import minibase.catalog.CatalogKey;

/**
 * In the schema of query expressions, a foreign key is represented as a key consisting of the referencing
 * column references and a key consisting of the referenced column references.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 * @version 1.0
 */
public class SchemaForeignKey extends AbstractForeignKey<SchemaKey> {

   /** Unresolved catalog key. */
   @SuppressWarnings("unused")
   private final CatalogKey unresolved;

   /**
    * Constructs a new schema foreign key using the given (resolved) referencing schema key and the given
    * (unresolved) catalog key.
    *
    * @param referencing
    *           referencing key
    * @param unresolved
    *           referenced key
    */
   public SchemaForeignKey(final SchemaKey referencing, final CatalogKey unresolved) {
      super(referencing, null);
      this.unresolved = unresolved;
   }

   /**
    * Returns whether this schema key has been successfully resolved, i.e., whether a schema key consisting of
    * the referencing column references has been created.
    *
    * @return {@code true} if this key is resolved, {@code false} otherwise
    */
   public boolean isResolved() {
      return this.getReferencedColumns() != null;
   }
}

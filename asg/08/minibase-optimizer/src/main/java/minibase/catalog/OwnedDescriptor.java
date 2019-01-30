/*
 * @(#)OwnedDescriptor.java   1.0   Aug 30, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

/**
 * Interface of all descriptors in the Minibase system catalog that are owned by another catalog object.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
public interface OwnedDescriptor extends Descriptor {

   /**
    * Returns the identifier of the system catalog object that owns this descriptor.
    *
    * @return owner identifier
    */
   int getOwnerID();
}

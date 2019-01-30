/*
 * @(#)ResourceUtil.java   1.0   Jun 14, 2016
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Utility class for handling resource files.
 *
 * @author Leo Woerteler &lt;leonard.woerteler@uni-konstanz.de&gt;
 */
public final class ResourceUtil {
   /** Hidden constructor. */
   private ResourceUtil() {
   }

   /**
    * Loads the given resource with this class' class loader.
    *
    * @param resource resource to load
    * @return resource as a stream
    */
   public static InputStream getResourceAsStream(final String resource) {
      final InputStream in = ResourceUtil.class.getClassLoader().getResourceAsStream(resource);
      if (in != null) {
         return in;
      }
      try {
         return new FileInputStream(resource);
      } catch (final FileNotFoundException e) {
         final String path = new File(".").getAbsolutePath();
         throw new Error(path, e);
      }
   }
}

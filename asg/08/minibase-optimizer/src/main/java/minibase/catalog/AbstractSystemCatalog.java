/*
 * @(#)AbstractSystemCatalog.java   1.0   Aug 29, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstract common superclass for system catalog implementations. Apart from utility methods, this class
 * implements a caching framework for system catalog descriptors.
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni-konstanz.de&gt;
 */
@SuppressWarnings("unchecked")
public abstract class AbstractSystemCatalog implements SystemCatalog {

   /** Cache of descriptors, indexed by identifier. */
   private final Map<Class< ? extends Descriptor>, Map<Integer, Descriptor>> descriptorCache;

   /** Cache of descriptor identifiers, indexed by name. */
   private final Map<Class< ? extends NamedDescriptor>, Map<String, Integer>> idCache;

   /** Cache of owned descriptor identifier lists, indexed by table name. */
   private final Map<Integer, Map<Class< ? extends OwnedDescriptor>, List<Integer>>> ownedIdCache;

   /**
    * Creates a new system catalog and initializes its internal data structures.
    */
   AbstractSystemCatalog() {
      this.descriptorCache = new HashMap<>();
      this.idCache = new HashMap<>();
      this.ownedIdCache = new HashMap<>();
   }

   @Override
   public final int createIndex(final IndexStatistics statistics, final String name, final IndexType type,
         final boolean clustered, final int[] keyColumnIDs, final int tableID) {
      final SortOrder[] keyOrders = new SortOrder[keyColumnIDs.length];
      switch (type) {
         case BTREE:
            throw new IllegalStateException("B+ tree index must specify sort order of key columns.");
         case SHASH:
            Arrays.fill(keyOrders, SortOrder.HASHED);
            break;
         case BITMAP:
         case BITMAP_JOIN:
         default:
            Arrays.fill(keyOrders, SortOrder.UNSORTED);
            break;
      }
      return this.createIndex(statistics, name, type, clustered, keyColumnIDs, keyOrders, tableID);
   }

   /**
    * Caches the given descriptor. The descriptor is indexed by the give type and its identifier. If the given
    * descriptor is a named descriptor, it is also indexed by its name. If the given descriptor is an owned
    * descriptor, it is also indexed by the identifier of its owner.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   void putDescriptor(final Class< ? extends Descriptor> type, final Descriptor descriptor) {
      this.indexDescriptorId(type, descriptor);
      if (descriptor instanceof NamedDescriptor) {
         this.indexDescriptorName((Class< ? extends NamedDescriptor>) type, (NamedDescriptor) descriptor);
      }
      if (descriptor instanceof OwnedDescriptor) {
         this.indexDescriptorOwner((Class< ? extends OwnedDescriptor>) type, (OwnedDescriptor) descriptor);
      }
   }

   /**
    * Caches the given descriptor. The descriptor is indexed by its type and identifier. If the given
    * descriptor is a named descriptor, it is also indexed by its name. If the given descriptor is an owned
    * descriptor, it is also indexed by the identifier of its owner.
    *
    * @param descriptor
    *           system catalog descriptor
    */
   void putDescriptor(final Descriptor descriptor) {
      this.putDescriptor(descriptor.getClass(), descriptor);
   }

   /**
    * Removes the given descriptor from the cache.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   void removeDescriptor(final Class< ? extends Descriptor> type, final Descriptor descriptor) {
      this.unindexDescriptorId(type, descriptor);
      if (descriptor instanceof NamedDescriptor) {
         this.unindexDescriptorName((Class< ? extends NamedDescriptor>) type, (NamedDescriptor) descriptor);
      }
      if (descriptor instanceof OwnedDescriptor) {
         this.unindexDescriptorOwner((Class< ? extends OwnedDescriptor>) type, (OwnedDescriptor) descriptor);
      }
   }

   /**
    * Removes the given descriptor from the cache.
    *
    * @param descriptor
    *           system catalog descriptor
    */
   void removeDescriptor(final Descriptor descriptor) {
      this.removeDescriptor(descriptor.getClass(), descriptor);
   }

   /**
    * Indexes the given system catalog descriptor by its identifier.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   private void indexDescriptorId(final Class< ? extends Descriptor> type, final Descriptor descriptor) {
      Map<Integer, Descriptor> descriptors = this.descriptorCache.get(type);
      if (descriptors == null) {
         descriptors = new HashMap<>();
         this.descriptorCache.put(type, descriptors);
      }
      descriptors.put(Integer.valueOf(descriptor.getCatalogID()), descriptor);
   }

   /**
    * Unindexes the given descriptor from the cache by its identifier.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   private void unindexDescriptorId(final Class< ? extends Descriptor> type, final Descriptor descriptor) {
      final Map<Integer, Descriptor> descriptors = this.descriptorCache.get(type);
      if (descriptors != null) {
         descriptors.remove(Integer.valueOf(descriptor.getCatalogID()));
      }
   }

   /**
    * Indexes the given named system catalog descriptor by its name.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   private void indexDescriptorName(final Class< ? extends NamedDescriptor> type,
         final NamedDescriptor descriptor) {
      Map<String, Integer> ids = this.idCache.get(type);
      if (ids == null) {
         ids = new HashMap<>();
         this.idCache.put(type, ids);
      }
      ids.put(descriptor.getName(), Integer.valueOf(descriptor.getCatalogID()));
   }

   /**
    * Unindexes the given named system catalog descriptor by its name.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   private void unindexDescriptorName(final Class< ? extends NamedDescriptor> type,
         final NamedDescriptor descriptor) {
      final Map<String, Integer> ids = this.idCache.get(type);
      if (ids != null) {
         ids.remove(descriptor.getName());
      }
   }

   /**
    * Indexes the given column system catalog descriptor by its table identifier.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   private void indexDescriptorOwner(final Class< ? extends OwnedDescriptor> type,
         final OwnedDescriptor descriptor) {
      final Integer id = Integer.valueOf(descriptor.getOwnerID());
      Map<Class< ? extends OwnedDescriptor>, List<Integer>> descriptorTypes = this.ownedIdCache.get(id);
      if (descriptorTypes == null) {
         descriptorTypes = new HashMap<>();
         this.ownedIdCache.put(id, descriptorTypes);
      }
      List<Integer> ids = descriptorTypes.get(type);
      if (ids == null) {
         ids = new ArrayList<>();
         descriptorTypes.put(type, ids);
      }
      ids.add(Integer.valueOf(descriptor.getCatalogID()));
   }

   /**
    * Indexes the given column system catalog descriptor by its table identifier.
    *
    * @param type
    *           system catalog descriptor index type
    * @param descriptor
    *           system catalog descriptor
    */
   private void unindexDescriptorOwner(final Class< ? extends OwnedDescriptor> type,
         final OwnedDescriptor descriptor) {
      final Integer id = Integer.valueOf(descriptor.getOwnerID());
      final Map<Class< ? extends OwnedDescriptor>, List<Integer>> descriptorTypes = this.ownedIdCache.get(id);
      if (descriptorTypes != null) {
         final List<Integer> ids = descriptorTypes.get(type);
         if (ids != null) {
            ids.remove(Integer.valueOf(descriptor.getCatalogID()));
         }
      }
   }

   /**
    * Looks up if there is a system catalog descriptor with the given identifier in the cache.
    *
    * @param type
    *           descriptor type
    * @param descriptorID
    *           descriptor identifier
    * @param <T>
    *           descriptor type parameter
    * @return system catalog descriptor
    */
   <T extends Descriptor> Optional<T> getDescriptor(final Class<T> type, final int descriptorID) {
      final Map<Integer, Descriptor> descriptors = this.descriptorCache.get(type);
      if (descriptors != null) {
         return Optional.ofNullable((T) descriptors.get(Integer.valueOf(descriptorID)));
      }
      return Optional.empty();
   }

   /**
    * Looks up if there is a system catalog descriptor with the given name in the cache.
    *
    * @param type
    *           named descriptor type
    * @param name
    *           descriptor name
    * @param <T>
    *           descriptor type parameter
    * @return system catalog descriptor
    */
   <T extends NamedDescriptor> Optional<T> getDescriptor(final Class<T> type, final String name) {
      final Map<String, Integer> ids = this.idCache.get(type);
      if (ids != null) {
         final Integer id = ids.get(name);
         if (id != null) {
            return this.getDescriptor(type, id.intValue());
         }
      }
      return Optional.empty();
   }

   /**
    * Looks up if there is a system catalog descriptor owned by the given owner and with the given name.
    *
    * @param type
    *           named descriptor type
    * @param ownerID
    *           identifier of the owner descriptor
    * @param name
    *           name of the descriptor
    * @param <T>
    *           descriptor type parameter
    * @return system catalog descriptor
    */
   <T extends NamedDescriptor> Optional<T> getDescriptor(final Class<T> type, final int ownerID,
         final String name) {
      if (OwnedDescriptor.class.isAssignableFrom(type)) {
         for (final OwnedDescriptor descriptor : this.getDescriptors(
               (Class< ? extends OwnedDescriptor>) type, ownerID)) {
            if (descriptor instanceof NamedDescriptor) {
               final NamedDescriptor namedDescriptor = (NamedDescriptor) descriptor;
               if (namedDescriptor.getName().equals(name)) {
                  return Optional.of((T) namedDescriptor);
               }
            }
         }
         return Optional.empty();
      }
      throw new IllegalStateException("Type " + type.getSimpleName() + " is not assignable from "
            + OwnedDescriptor.class.getSimpleName() + ".");
   }

   /**
    * Returns all system catalog descriptors of the given type that are currently in the system catalog.
    *
    * @param type
    *           descriptor type
    * @param <T>
    *           descriptor type parameter
    * @return system catalog descriptors
    */
   <T extends Descriptor> Collection<T> getDescriptors(final Class<T> type) {
      final Map<Integer, Descriptor> descriptors = this.descriptorCache.get(type);
      if (descriptors != null) {
         return (Collection<T>) Collections.unmodifiableCollection(descriptors.values());
      }
      return Collections.emptySet();
   }

   /**
    * Returns all system catalog descriptors of the given type that are owned by the system catalog object
    * with the given name.
    *
    * @param type
    *           descriptor type
    * @param ownerID
    *           owner identifier
    * @param <T>
    *           descriptor type parameter
    * @return owned system catalog descriptors
    */
   <T extends OwnedDescriptor> List<T> getDescriptors(final Class<T> type, final int ownerID) {
      final Map<Class< ? extends OwnedDescriptor>, List<Integer>> descriptorTypes = this.ownedIdCache
            .get(Integer.valueOf(ownerID));
      if (descriptorTypes != null && descriptorTypes.get(type) != null) {
         return Collections.unmodifiableList(descriptorTypes.get(type).stream()
               .map(i -> this.getDescriptor(type, i.intValue()).get()).collect(Collectors.<T> toList()));
      }
      return Collections.emptyList();
   }

   /**
    * Checks if the given lists of column IDs form a valid foreign key.
    *
    * @param keyColumnIDs
    *           referencing columns of the foreign key
    * @param refColumnIDs
    *           referenced columns of the foreign key
    * @param tableConstraint
    *           {@code true} if the foreign key is a table constraint, {@code false} otherwise
    */
   void assertForeignKeyValid(final int[] keyColumnIDs, final int[] refColumnIDs,
         final boolean tableConstraint) {
      if (keyColumnIDs.length < 1 || refColumnIDs.length < 1) {
         throw new IllegalArgumentException("Foreign key constaint must reference at least one column.");
      }
      if (keyColumnIDs.length != refColumnIDs.length) {
         throw new IllegalArgumentException("The number of referencing and referenced columns must be equal.");
      }
      if (tableConstraint && (keyColumnIDs.length > 1 || refColumnIDs.length > 1)) {
         throw new IllegalArgumentException(
               "Column foreign key constraint must reference exactly one column.");
      }
   }

   /**
    * Checks that the given name is not already assigned to an existing constraint and throws an exception if
    * it is.
    *
    * @param name
    *           new constraint name
    */
   void assertConstraintNotExists(final String name) {
      if (name != null && this.getDescriptor(ConstraintDescriptor.class, name).isPresent()) {
         throw new IllegalStateException("Constraint " + name + " exists already.");
      }
   }

   /**
    * Checks that the given collection contains exactly one element and returns it.
    *
    * @param collection
    *           collection to check
    * @param <T>
    *           collection type
    * @return collection element
    */
   <T> T assertContainsOneOrNone(final Collection<T> collection) {
      if (collection.size() == 1) {
         return collection.iterator().next();
      } else if (collection.size() > 1) {
         throw new IllegalStateException("Collection " + collection + " cannot own more than one element.");
      }
      return null;
   }

   @Override
   public String toSQL() {
      final StringBuilder builder = new StringBuilder();
      for (final TableDescriptor table : this.getTables()) {
         builder.append(table.toSQL());
         builder.append("\n");
      }
      return builder.toString();
   }
}

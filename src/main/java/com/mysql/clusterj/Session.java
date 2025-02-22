/*
   Copyright (c) 2010, 2011, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package com.mysql.clusterj;

import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDefinition;
import com.mysql.ndbjtie.ndbapi.NdbEventOperation;

import java.util.List;

/** Session is the primary user interface to the cluster.
 *
 */
public interface Session {

    /** Get a QueryBuilder. 
     * @return the query builder
     */
    QueryBuilder getQueryBuilder();

    /** Create a Query from a QueryDefinition. 
     * @param qd the query definition
     * @return the query instance
     */
    <T> Query<T> createQuery(QueryDefinition<T> qd);

    /** Find a specific instance by its primary key.
     * The key must be of the same type as the primary key defined
     * by the table corresponding to the cls parameter. 
     * The key parameter is the wrapped version of the
     * primitive type of the key, e.g. Integer for INT key types,
     * Long for BIGINT key types, or String for char and varchar types.
     * 
     * For multi-column primary keys, the key parameter is an Object[],
     * each element of which is a component of the primary key.
     * The elements must be in the order of declaration of the columns
     * (not necessarily the order defined in the CONSTRAINT ... PRIMARY KEY 
     * clause) of the CREATE TABLE statement.
     * 
     * @param cls the interface or dynamic class to find an instance of
     * @param key the key of the instance to find
     * @return the instance of the interface or dynamic class with the specified key
     */
    <T> T find(Class<T> cls, Object key);

    /** Create an instance of an interface or dynamic class that maps to a table.
     * @param cls the interface for which to create an instance
     * @return an instance that implements the interface
     */
    <T> T newInstance(Class<T> cls);

    /** Create an instance of an interface or dynamic class that maps to a table
     * and set the primary key of the new instance. The new instance
     * can be used to create, delete, or update a record in the database.
     * @param cls the interface for which to create an instance
     * @return an instance that implements the interface
     */
    <T> T newInstance(Class<T> cls, Object key);

    /** Insert the instance into the database.
     * If the instance already exists in the database, an exception is thrown.
     * @see Session#savePersistent(java.lang.Object)
     * @param instance the instance to insert
     * @return the instance
     */
    <T> T makePersistent(T instance);

    /** Load the instance from the database into memory. Loading
     * is asynchronous and will be executed when an operation requiring
     * database access is executed: find, flush, or query. The instance must
     * have been returned from find or query; or
     * created via session.newInstance and its primary key initialized.
     * @param instance the instance to load
     * @return the instance
     * @see #found(Object)
     */
    <T> T load(T instance);

    /** Was the row corresponding to this instance found in the database?
     * @param instance the instance corresponding to the row in the database
     * @return <ul><li>null if the instance is null or was created via newInstance and never loaded;
     * </li><li>true if the instance was returned from a find or query
     * or created via newInstance and successfully loaded;
     * </li><li>false if the instance was created via newInstance and not found.
     * </li></ul>
     * @see #load(Object)
     * @see #newInstance(Class, Object)
     */
    Boolean found(Object instance);
    
    /** Insert the instance into the database. This method has identical
     * semantics to makePersistent.
     * @param instance the instance to insert
     */
    void persist(Object instance);

    /** Insert the instances into the database.
     * @param instances the instances to insert.
     * @return the instances
     */
    Iterable<?> makePersistentAll(Iterable<?> instances);

    /** Delete an instance of a class from the database given its primary key.
     * For single-column keys, the key parameter is a wrapper (e.g. Integer).
     * For multi-column keys, the key parameter is an Object[] in which
     * elements correspond to the primary keys in order as defined in the schema.
     * @param cls the interface or dynamic class
     * @param key the primary key
     */
    public <T> void deletePersistent(Class<T> cls, Object key);

    /** Delete the instance from the database. Only the id field is used
     * to determine which instance is to be deleted.
     * If the instance does not exist in the database, an exception is thrown.
     * @param instance the instance to delete
     */
    void deletePersistent(Object instance);

    /** Delete the instance from the database. This method has identical
     * semantics to deletePersistent.
     * @param instance the instance to delete
     */
    void remove(Object instance);

    /** Delete all instances of this class from the database.
     * No exception is thrown even if there are no instances in the database.
     * @param cls the interface or dynamic class
     * @return the number of instances deleted
     */
    <T> int deletePersistentAll(Class<T> cls);

    /** Delete all parameter instances from the database.
     * @param instances the instances to delete
     */
    void deletePersistentAll(Iterable<?> instances);

    /** Update the instance in the database without necessarily retrieving it.
     * The id field is used to determine which instance is to be updated.
     * If the instance does not exist in the database, an exception is thrown.
     * This method cannot be used to change the primary key.
     * @param instance the instance to update
     */
    void updatePersistent(Object instance);

    /** Update all parameter instances in the database.
     * @param instances the instances to update
     */
    void updatePersistentAll(Iterable<?> instances);

    /** Save the instance in the database without checking for existence.
     * The id field is used to determine which instance is to be saved.
     * If the instance exists in the database it will be updated.
     * If the instance does not exist, it will be created.
     * @param instance the instance to update
     */
    <T> T savePersistent(T instance);

    /** Update all parameter instances in the database.
     * @param instances the instances to update
     */
    Iterable<?> savePersistentAll(Iterable<?> instances);

    /** Get the current {@link Transaction}.
     * @return the transaction
     */
    Transaction currentTransaction();

    /** Close this session.
     * 
     */
    void close();

    /** Is this session closed?
     *
     * @return true if the session is closed
     */
    boolean isClosed();

    /** Flush deferred changes to the back end. Inserts, deletes, loads, and
     * updates are sent to the
     * back end.
     */
    void flush();

    /** Set the partition key for the next transaction. 
     * The key must be of the same type as the primary key defined
     * by the table corresponding to the cls parameter. 
     * The key parameter is the wrapped version of the
     * primitive type of the key, e.g. Integer for INT key types,
     * Long for BIGINT key types, or String for char and varchar types.
     * 
     * For multi-column primary keys, the key parameter is an Object[],
     * each element of which is a component of the primary key.
     * The elements must be in the order of declaration of the columns
     * (not necessarily the order defined in the CONSTRAINT ... PRIMARY KEY 
     * clause) of the CREATE TABLE statement.
     * 
     * @throws ClusterJUserException if a transaction is enlisted
     * @throws ClusterJUserException if a partition key is null
     * @throws ClusterJUserException if called twice in the same transaction
     * @throws ClusterJUserException if a partition key is the wrong type
     * @param key the primary key of the mapped table
     */
    void setPartitionKey(Class<?> cls, Object key);

    /** Set the lock mode for read operations. This will take effect immediately
     * and will remain in effect until this session is closed or this method
     * is called again.
     * @param lockmode the LockMode
     */
    void setLockMode(LockMode lockmode);

    /** Mark the field in the object as modified so it is flushed.
     *
     * @param instance the persistent instance
     * @param fieldName the field to mark as modified
     */
    void markModified(Object instance, String fieldName);

    /** Unload the schema definition for a class. This must be done after the schema
     * definition has changed in the database due to an alter table command.
     * The next time the class is used the schema will be reloaded.
     * @param cls the class for which the schema is unloaded
     * @return the name of the schema that was unloaded
     */
    String unloadSchema(Class<?> cls);

    /**
     * Create a subscription to an event defined in the database.
     *
     * @param eventName
     *        unique identifier of the event
     *
     * @return Object representing an event operation, NULL on failure
     */
    public EventOperation createEventOperation(String eventName);

    /**
     * Create and register an event with the database. The event will be identified by the provided event name and
     * will be associated with the specified table.
     *
     * @param eventName The unique name to identify the event with.
     * @param tableName The table with which the event should be associated.
     * @param eventColumns The columns that are being monitored for the event.
     * @param tableEvents The events that this event should listen for.
     * @param force This is passed to the dropTable() function if the event we're trying to create already exists,
     *              and we must drop the existing event first.
     * @param recreateIfExists If true, then drop and recreate the event if it already exists.
     */
    public Event createAndRegisterEvent(String eventName,
                                        String tableName,
                                        String[] eventColumns,
                                        TableEvent[] tableEvents,
                                        int force,
                                        boolean recreateIfExists);

    /**
     * Create and register an event with the database. The event will be identified by the provided event name and
     * will be associated with the specified table. The event will be created on/for ALL table columns of the
     * associated table.
     *
     * @param eventName The unique name to identify the event with.
     * @param tableName The table with which the event should be associated.
     * @param tableEvents The events that this event should listen for.
     * @param force This is passed to the dropTable() function if the event we're trying to create already exists,
     *              and we must drop the existing event first.
     * @param recreateIfExists If true, then drop and recreate the event if it already exists.
     */
    public Event createAndRegisterEvent(String eventName,
                                        String tableName,
                                        TableEvent[] tableEvents,
                                        int force,
                                        boolean recreateIfExists);

    /**
     * Drop a subscription to an event.
     *
     * @param eventOp
     *        Event operation
     *
     * @return 0 on success
     */
    public boolean dropEventOperation(EventOperation eventOp);

    /**
     * Wait for an event to occur. Will return as soon as an event
     * is available on any of the created events.
     *
     * @param aMillisecondNumber
     *        maximum time to wait
     * aMillisecondNumber < 0 will cause a long wait
     * @param latestGCI
     *        if a valid pointer is passed to a 64-bit integer it will be set
     *        to the latest polled GCI. If a cluster failure is detected it
     *        will be set to NDB_FAILURE_GCI.
     *
     * @return True if events available, false if no events available.
     */
    public boolean pollEvents(int aMillisecondNumber, long[] latestGCI);

    /**
     * This is a backward compatibility wrapper to nextEvent2().
     * Returns an event operation that has data after a pollEvents,
     * NULL if the queue is empty.
     * It maintains the old behaviour :
     * - returns NULL for inconsistent epochs. Therefore, it is important
     *   to call isConsistent(Uint64& gci) to check for inconsistency,
     *   after nextEvent() returns NULL.
     * - will not have empty epochs in the event queue (i.e. remove them),
     * - exits the process when it encounters an event data
     *   representing an event buffer overflow.
     */
    public EventOperation nextEvent();

    /**
     * Return the event with the given name.
     *
     * @param eventName The name of the event.
     * @return The event.
     */
    public Event getEvent(String eventName);

    /**
     * Drop the event identified by the given name.
     * @param eventName The unique name of the event to drop.
     * @param force If 1, then do not check if event exists before trying to delete it.
     * @return True if the event was dropped, false if the event did not exist in the first place.
     */
    public boolean dropEvent(String eventName, int force);

    /**
     * Return a list containing the names of all active/registered NDB events with the current database.
     * @return List of NDB Event names.
     */
    public List<String> getEventNames();

    /** Release resources associated with an instance. The instance must be a domain object obtained via
     * session.newInstance(T.class), find(T.class), or query; or Iterable<T>, or array T[].
     * Resources released can include direct buffers used to hold instance data.
     * Released resources may be returned to a pool.
     * @param obj a domain object of type T, an Iterable<T>, or array T[]
     * @return the input parameter
     * @throws ClusterJUserException if the instance is not a domain object T, Iterable<T>, or array T[],
     * or if the object is used after calling this method.
     */
    <T> T release(T obj);
}

package com.mysql.clusterj.core.store;

import com.mysql.clusterj.TableEvent;
import com.mysql.ndbjtie.ndbapi.NdbErrorConst;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;

import java.nio.ByteBuffer;

/**
 * Wrapper around {@link com.mysql.ndbjtie.ndbapi.NdbEventOperationConst}.
 */
public interface EventOperation {
    public int isOverrun();

    /**
     * In the current implementation a nodefailiure may cause loss of events,
     * in which case isConsistent() will return false
     */
    public boolean isConsistent();

    /**
     * Query for occurred event type. This is a backward compatibility
     * wrapper for getEventType2(). Since it is called after nextEvent()
     * returned a non-NULL event operation after filtering exceptional epoch
     * event data, it should not see the exceptional event data types:
     * TE_EMPTY, TE_INCONSISTENT, TE_OUT_OF_MEMORY
     *
     * Only valid after Ndb::nextEvent() has been called and returned a non-NULL value
     *
     * @return type of event
     */
    public TableEvent getEventType();

    /**
     * Check if table name has changed, for event TE_ALTER
     */
    public boolean tableNameChanged();

    /**
     * Check if table frm has changed, for event TE_ALTER
     */
    public boolean tableFrmChanged();

    /**
     * Check if table fragmentation has changed, for event TE_ALTER
     */
    public boolean tableFragmentationChanged();

    /**
     * Get the state of the event operation.
     *      CREATED = 0,
     *      EXECUTING = 1,
     *      DROPPED = 2,
     *      ERROR = 3
     * @return Integer representing the state.
     */
    public int getState();

    /**
     * Check if the underlying, internal NDBEventOperation objects are the same for
     * two instances of NdbEventOperationImpl. Equality is checked by reference (i.e., '==').
     * @return True if the underlying, internal NDBEventOperation objects are the same.
     */
    public boolean underlyingEquals(EventOperation other);

    /**
     * Check if table range partition list name has changed, for event TE_ALTER
     */
    public boolean tableRangeListChanged();

    /**
     * Retrieve the GCI of the latest retrieved event
     *
     * @return GCI number
     *
     * This is a wrapper to getEpoch() for backward compatibility.
     */
    public long getGCI();

    /**
     * Retrieve the AnyValue of the latest retrieved event
     *
     * @return AnyValue
     */
    public int getAnyValue();

    /**
     * Retrieve the complete GCI in the cluster (not necessarily
     * associated with an event)
     *
     * @return GCI number
     */
    public long getLatestGCI();

    /**
     * Activates the NdbEventOperation to start receiving events. The
     * changed attribute values may be retrieved after Ndb::nextEvent()
     * has returned not NULL. The getValue() methods must be called
     * prior to execute().
     */
    public void execute();

    public NdbErrorConst /*_const NdbError &_*/ getNdbError();

    /**
     * Defines a retrieval operation of an attribute value.
     * The NDB API allocate memory for the NdbRecAttr object that
     * will hold the returned attribute value.
     *
     *       Note that it is the application's responsibility
     *       to allocate enough memory for aValue (if non-NULL).
     *       The buffer aValue supplied by the application must be
     *       aligned appropriately.  The buffer is used directly
     *       (avoiding a copy penalty) only if it is aligned on a
     *       4-byte boundary and the attribute size in bytes
     *       (i.e. NdbRecAttr::attrSize() times NdbRecAttr::arraySize() is
     *       a multiple of 4).
     *
     *       There are two versions, getValue() and
     *       getPreValue() for retrieving the current and
     *       previous value respective.
     *
     *       This method does not fetch the attribute value from
     *       the database!  The NdbRecAttr object returned by this method
     *       is <em>not</em> readable/printable before the call to execute()
     *       has been made and Ndb::nextEvent() has returned not NULL.
     *       If a specific attribute has not changed the corresponding
     *       NdbRecAttr will be in state UNDEFINED.  This is checked by
     *       NdbRecAttr::isNULL() which then returns -1.
     *
     * @param anAttrName  Attribute name
     *        aValue      If this is non-NULL, then the attribute value
     *                    will be returned in this parameter.<br>
     *                    If NULL, then the attribute value will only
     *                    be stored in the returned NdbRecAttr object.
     *
     *                    Note: I allocate this buffer directly within the call to getValue() and store it on
     *                    the ClusterJ RecordAttr object.
     *
     * @return            An NdbRecAttr object to hold the value of
     *                    the attribute, or a NULL pointer
     *                    (indicating error).
     */
    public RecordAttr getValue(String anAttrName);

    /**
     * See getValue().
     */
    public RecordAttr getPreValue(String anAttrName);
}

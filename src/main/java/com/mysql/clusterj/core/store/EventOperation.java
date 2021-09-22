package com.mysql.clusterj.core.store;

import com.mysql.ndbjtie.ndbapi.NdbErrorConst;

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
    public int getEventType();

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
}

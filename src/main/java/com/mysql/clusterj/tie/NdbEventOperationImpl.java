package com.mysql.clusterj.tie;

import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.Db;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;
import com.mysql.ndbjtie.ndbapi.NdbErrorConst;
import com.mysql.ndbjtie.ndbapi.NdbEventOperation;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;

import java.nio.ByteBuffer;

public class NdbEventOperationImpl implements EventOperation {
    /** My message translator */
    static final I18NHelper local = I18NHelper
            .getInstance(NdbEventOperationImpl.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory()
            .getInstance(NdbEventOperationImpl.class);

    /** The db for this operation */
    protected DbImpl db;

    private final NdbEventOperation ndbEventOperation;

    private final TableEvent eventType;

    public NdbEventOperationImpl(NdbEventOperation ndbEventOperation, Db db) {
        this.db = (DbImpl)db;
        this.eventType = TableEvent.convert(ndbEventOperation.getEventType());
        this.ndbEventOperation = ndbEventOperation;
    }

    public int isOverrun() {
        return ndbEventOperation.isOverrun();
    }

    /**
     * In the current implementation a node failure may cause loss of events,
     * in which case isConsistent() will return false
     */
    public boolean isConsistent() {
        return ndbEventOperation.isConsistent();
    }

    /**
     * Query for occurred event type. This is a backward compatibility
     * wrapper for getEventType2(), which is part of the C++ API.
     * Since it is called after nextEvent returned a non-NULL event operation
     * after filtering exceptional epoch event data, it should not see the exceptional
     * event data types: TE_EMPTY, TE_INCONSISTENT, TE_OUT_OF_MEMORY
     *
     * Only valid after Ndb::nextEvent() has been called and returned a non-NULL value
     *
     * @return type of event
     */
    public TableEvent getEventType() {
        return eventType;
    }

    /**
     * Check if table name has changed, for event TE_ALTER.
     */
    public boolean tableNameChanged() {
        return ndbEventOperation.tableNameChanged();
    }

    /**
     * Check if table frm has changed, for event TE_ALTER.
     */
    public boolean tableFrmChanged() {
        return ndbEventOperation.tableFrmChanged();
    }

    /**
     * Check if table fragmentation has changed, for event TE_ALTER.
     */
    public boolean tableFragmentationChanged() {
        return ndbEventOperation.tableFragmentationChanged();
    }

    /**
     * Check if table range partition list name has changed, for event TE_ALTER.
     */
    public boolean tableRangeListChanged() {
        return ndbEventOperation.tableRangeListChanged();
    }

    /**
     * Retrieve the GCI of the latest retrieved event.
     *
     * GCI refers to "Global Checkpoint ID". This marks the point in the REDO log where a GCP took place.
     * A GCP (or global checkpoint) occurs every few seconds, when transactions for all nodes are synchronized
     * and the REDO log is flushed to disk.
     * @return GCI number
     *
     * This is a wrapper to getEpoch() for backward compatibility.
     */
    public long getGCI() {
        return ndbEventOperation.getGCI();
    }

    /**
     * Retrieve the AnyValue of the latest retrieved event
     *
     * @return AnyValue
     */
    public int getAnyValue() {
        return ndbEventOperation.getAnyValue();
    }

    /**
     * Retrieve the complete GCI in the cluster (not necessarily associated with an event)
     *
     * @return GCI number
     */
    public long getLatestGCI() {
        return ndbEventOperation.getLatestGCI();
    }

    public void execute() {
        logger.debug("Executing event operation now...");

        int returnCode = ndbEventOperation.execute();

        if (returnCode > 0) {
            logger.error("Failed to execute the event operation.");
            handleError(returnCode);
        }
    }

    /**
     * Get the latest error.
     */
    public NdbErrorConst getNdbError() {
        return ndbEventOperation.getNdbError();
    }

    public RecordAttr getValue(String anAttrName) {
        ByteBuffer aValue = ByteBuffer.allocateDirect(64); // Big enough.

        logger.debug("Getting value for operation. Attribute name: " + anAttrName);
        NdbRecAttr ndbRecAttr = ndbEventOperation.getValue(anAttrName, aValue);

        return new NdbRecordAttrImpl(ndbRecAttr, null, aValue);
    }

    public RecordAttr getPreValue(String anAttrName) {
        ByteBuffer aValue = ByteBuffer.allocateDirect(64); // Big enough.

        logger.debug("Getting value for operation. Attribute name: " + anAttrName);
        NdbRecAttr ndbRecAttr = ndbEventOperation.getValue(anAttrName, aValue);

        return new NdbRecordAttrImpl(ndbRecAttr, null, aValue);
    }

    protected void handleError() {
        NdbErrorConst ndbError = getNdbError();
        String detail = db.getNdbErrorDetail(ndbError);
        Utility.throwError(0, ndbError, detail);
    }

    protected void handleError(int returnCode) {
        NdbErrorConst ndbError = getNdbError();
        String detail = db.getNdbErrorDetail(ndbError);
        Utility.throwError(returnCode, ndbError, detail);
    }
}

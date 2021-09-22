package com.mysql.clusterj.core.store;

import com.mysql.clusterj.EventDurability;
import com.mysql.clusterj.EventReport;

/**
 * Wrapper around {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst}.
 */
public interface Event {
    public String getName();

    public String getTableName();

    public boolean getTableEvent(int tableEvent);

    public Table getTable();

    public EventDurability getDurability();

    public EventReport getReport();

    public int getNoOfEventColumns();
}

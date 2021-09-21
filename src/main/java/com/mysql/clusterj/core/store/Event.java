package com.mysql.clusterj.core.store;

import com.mysql.clusterj.EventDurability;

/**
 * Wrapper around {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst}.
 */
public interface Event {
    public String getName();

    public String getTableName();

    public boolean getTableEvent(int tableEvent);

    public Table getTable();

    public EventDurability getDurability();

    public int getReport();

    public int getNoOfEventColumns();
}

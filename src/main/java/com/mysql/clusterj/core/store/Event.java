package com.mysql.clusterj.core.store;

import com.mysql.clusterj.EventDurability;
import com.mysql.clusterj.EventReport;
import com.mysql.clusterj.TableEvent;

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

    /**
     * Return the number of columns for this event.
     */
    public int getNoOfEventColumns();

    /**
     * Get the currently-set columns for this event.
     */
    public String[] getEventColumns();

    /**
     * Set the columns for this event.
     *
     * This should be done BEFORE registering it with the server.
     */
    public void setEventColumns(String[] eventColumns);

    /**
     * Get the list of events that this event listens for.
     */
    public TableEvent[] getTableEvents();

    /**
     * Set the NDB events to listen to for this event.
     *
     * This should be done BEFORE registering it with the server.
     */
    public void setTableEvents(TableEvent[] tableEvents);
}

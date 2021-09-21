package com.mysql.clusterj.tie;

import com.mysql.clusterj.EventDurability;
import com.mysql.clusterj.EventReport;
import com.mysql.clusterj.core.store.Column;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.Table;
import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;
import com.mysql.ndbjtie.ndbapi.NdbDictionary;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.TableConst;
import javafx.scene.control.Toggle;

/**
 * Wrapper around {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.Event}.
 */
public class EventImpl implements Event {
    /** My message translator */
    static final I18NHelper local = I18NHelper
            .getInstance(EventImpl.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory()
            .getInstance(EventImpl.class);

    private final String name;
    private final String tableName;
    private final EventDurability eventDurability;
    private final EventReport eventReport;
    private final int numEventColumns;

    private final Table table;

    public EventImpl(EventConst ndbEvent, Table table) {
        this.name = ndbEvent.getName();
        this.tableName = ndbEvent.getTableName();
        this.eventDurability = EventDurability.convert(ndbEvent.getDurability());
        this.eventReport = EventReport.convert(ndbEvent.getReport());
        this.numEventColumns = ndbEvent.getNoOfEventColumns();

        this.table = table;


        logger.debug("Created Event: " + this.toString());
    }

    /**
     * Get unique identifier for the event.
     */
    public String getName() {
        return name;
    }

    /**
     * Get table name for events.
     *
     * @return table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Check if a specific table event will be detected.
     */
    public boolean getTableEvent(int tableEvent) {
        return false;
    }

    /**
     * Get table that the event is defined on.
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get durability of the event
     */
    public EventDurability getDurability() {
        return eventDurability;
    }

    public int getReport() {
        return EventReport.convert(eventReport);
    }

    public int getNoOfEventColumns() {
        return numEventColumns;
    }

    @Override
    public String toString() {
        return "EventImpl[name=" + name + ", tableName=" + tableName + ", eventDurability=" + eventDurability.name()
                + ", eventReport=" + eventReport.name() + ", numEventColumns=" + numEventColumns + ", table=" + table;
    }
}

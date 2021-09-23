package com.mysql.clusterj.tie;

import com.mysql.clusterj.EventDurability;
import com.mysql.clusterj.EventReport;
import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.Table;
import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;
import com.sun.istack.internal.NotNull;

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
    //private final int numEventColumns;

    private final Table table;

    private String[] eventColumns;

    private TableEvent[] tableEvents;

    /**
     * The NDB Event that this is wrapping.
     */
    //private final EventConst ndbEvent;

    public EventImpl(
            String eventName,
            EventDurability eventDurability,
            EventReport eventReport,
            @NotNull Table table,
            String[] eventColumns,
            TableEvent[] tableEvents) {
        this.name = eventName;
        this.tableName = table.getName();
        this.eventDurability = eventDurability; // EventDurability.convert(ndbEvent.getDurability());
        this.eventReport = eventReport; //EventReport.convert(ndbEvent.getReport());
        this.eventColumns = eventColumns;
        this.tableEvents = tableEvents;

        //this.ndbEvent = ndbEvent;
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

    public EventReport getReport() {
        return eventReport;
    }

    public int getNoOfEventColumns() {
        return this.eventColumns.length;
    }

    public String[] getEventColumns() {
        return this.eventColumns;
    }

    public void setEventColumns(String[] eventColumns) {
        this.eventColumns = eventColumns;
    }

    public TableEvent[] getTableEvents() {
        return this.tableEvents;
    }

    public void setTableEvents(TableEvent[] tableEvents) {
        this.tableEvents = tableEvents;
    }

    @Override
    public String toString() {
        return "EventImpl[name=" + name + ", tableName=" + tableName + ", eventDurability=" + eventDurability.name()
                + ", eventReport=" + eventReport.name() + ", table=" + table;
    }
}

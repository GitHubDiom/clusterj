package com.mysql.clusterj;

/**
 * Specifies the type of database operations an Event listens to.
 */
public enum TableEvent {
    INSERT,             // Insert event on table
    DELETE,             // Delete event on table
    UPDATE,             // Update event on table
    DROP,               // Drop of table
    ALTER,              // Alter of table
    CREATE,             // Create of table
    GCP_COMPLETE,       // GCP is complete
    CLUSTER_FAILURE,    // Cluster is unavailable
    STOP,               // Stop of event operation
    NODE_FAILURE,       // Node failed
    SUBSCRIBE,          // Node subscribes
    UNSUBSCRIBE,        // Node unsubscribes
    ALL;                // Any/all event on table (not relevant when events are received)

    /**
     * Convert from the integer representation of a TableEvent to the Java enum.
     *
     * The integer values come from the {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst.TableEvent}
     * interface defined in {@link com.mysql.ndbjtie.ndbapi.NdbDictionary}.
     *
     * @param event Integer representation of a TableEvent.
     * @return The corresponding Java enum.
     */
    public TableEvent convert(int event) {
        switch (event) {
            case 1 << 0:
                return INSERT;
            case 1 << 1:
                return DELETE;
            case 1 << 2:
                return UPDATE;
            case 1 << 4:
                return DROP;
            case 1 << 5:
                return ALTER;
            case 1 << 6:
                return CREATE;
            case 1 << 7:
                return GCP_COMPLETE;
            case 1 << 8:
                return CLUSTER_FAILURE;
            case 1 << 9:
                return STOP;
            case 1 << 10:
                return NODE_FAILURE;
            case 1 << 11:
                return SUBSCRIBE;
            case 1 << 12:
                return UNSUBSCRIBE;
            case 1 << 0XFFFF:
                return ALL;
            default:
                throw new IllegalArgumentException("Unknown TableEvent: " + event);
        }
    }
}

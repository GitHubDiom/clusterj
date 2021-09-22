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
     * @param event Integer representation of the TableEvent.
     * @return The corresponding Java enum of the TableEvent.
     */
    public static TableEvent convert(int event) {
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

    /**
     * Convert from the Java enum representation of a TableEvent to the integer representation.
     *
     * The integer values come from the {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst.TableEvent}
     * interface defined in {@link com.mysql.ndbjtie.ndbapi.NdbDictionary}.
     *
     * @param event The Java enum representation of the TableEvent.
     * @return The corresponding Integer representation of the TableEvent.
     */
    public static int convert(TableEvent event) {
        switch (event) {
            case INSERT:
                return 1 << 0;
            case DELETE:
                return 1 << 1;
            case UPDATE:
                return 1 << 2;
            case DROP:
                return 1 << 4;
            case ALTER:
                return 1 << 5;
            case CREATE:
                return 1 << 6;
            case GCP_COMPLETE:
                return 1 << 7;
            case CLUSTER_FAILURE:
                return 1 << 8;
            case STOP:
                return 1 << 9;
            case NODE_FAILURE:
                return 1 << 10;
            case SUBSCRIBE:
                return 1 << 11;
            case UNSUBSCRIBE:
                return 1 << 12;
            case ALL:
                return 1 << 0XFFFF;
            default:
                throw new IllegalArgumentException("Unknown TableEvent: " + event);
        }
    }
}

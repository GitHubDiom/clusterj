package com.mysql.clusterj;

/**
 * Specifies reporting options for table events.
 */
public enum EventReport {
    UPDATED,
    ALL,            // Except not-updated blob inlines.
    SUBSCRIBE;

    /**
     * Convert from the integer representation of an EventReport to the Java enum.
     *
     * The integer values come from the {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst.EventReport}
     * interface defined in {@link com.mysql.ndbjtie.ndbapi.NdbDictionary}.
     *
     * @param report Integer representation of a TableEvent.
     * @return The corresponding Java enum.
     */
    public static EventReport convert(int report) {
        switch (report) {
            case 0:
                return UPDATED;
            case 1:
                return ALL;
            case 2:
                return SUBSCRIBE;
            default:
                throw new IllegalArgumentException("Unknown EventReport: " + report);
        }
    }

    /**
     * Convert an EventReport to its integer representation.
     *
     * The integer values were retrieved {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst.EventReport}.
     */
    public static int convert(EventReport report) {
        switch (report) {
            case UPDATED:
                return 0;
            case ALL:
                return 1;
            case SUBSCRIBE:
                return 2;
            default:
                return -1;
        }
    }
}

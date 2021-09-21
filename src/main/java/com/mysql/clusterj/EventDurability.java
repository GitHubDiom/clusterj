package com.mysql.clusterj;

/**
 * Specifies the durability of an Event.
 */
public enum EventDurability {
    UNDEFINED,  // Not supported.
    PERMANENT;  // All API's can use it.
                // It's still defined after a cluster system restart

    /**
     * Convert from the integer representation of an EventDurability to the Java enum.
     *
     * The integer values come from the {@link com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst.TableEvent}
     * interface defined in {@link com.mysql.ndbjtie.ndbapi.NdbDictionary}.
     *
     * @param durability Integer representation of a TableEvent.
     * @return The corresponding Java enum.
     */
    public static EventDurability convert(int durability) {
        switch (durability) {
            case 0:
                return UNDEFINED;
            case 3:
                return PERMANENT;
            default:
                throw new IllegalArgumentException("Unknown EventDurability: " + durability);
        }
    }
}

import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.tie.NdbEventOperationImpl;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;

import java.time.Instant;
import java.util.Properties;

public class ClusterJHopsFSAckTableTest {
    private static final String DEFAULT_CONNECT_STRING = "10.241.64.15:1186";
    private static final String DEFAULT_DATABASE = "hop_bram_vm";
    private static final String DEFAULT_TABLE_NAME = "write_acks_deployment0";
    private static final String DEFAULT_EVENT_NAME = "ack_table_watch0";

    private static final int DEFAULT_EVENT_LIMIT = 30;

    /**
     * Used to format the printing of event values for ACK events.
     */
    private static final String ACK_EVENT_FORMAT = "%36d%12d%12b%36d";

    /**
     * Columns for which we want to see values for ACK events.
     */
    private static final String[] ACK_EVENT_COLUMNS = new String[] {
            WriteAcknowledgementsTableDef.NAME_NODE_ID,       // bigint(20)
            WriteAcknowledgementsTableDef.DEPLOYMENT_NUMBER,  // int(11)
            WriteAcknowledgementsTableDef.ACKNOWLEDGED,       // tinyint(4)
            WriteAcknowledgementsTableDef.OPERATION_ID        // bigint(20)
    };

    /**
     * Used to format the printing of event values for INV events.
     */
    private static final String INV_EVENT_FORMAT = "%10d%10d%36d%36d%36d";

    private static final String[] INV_TABLE_EVENT_COLUMNS = new String[] {
            InvalidationTablesDef.INODE_ID,        // bigint(20)
            InvalidationTablesDef.PARENT_ID,       // bigint(20)
            InvalidationTablesDef.LEADER_ID,       // bigint(20), so it's a long.
            InvalidationTablesDef.TX_START,        // bigint(20), so it's a long.
            InvalidationTablesDef.OPERATION_ID,    // bigint(20), so it's a long.
    };

    public static final byte TRUE = 1;
    public static final byte FALSE = 0;

    public static byte convert(boolean val) {
        if (val) {
            return TRUE;
        } else {
            return FALSE;
        }
    }

    /**
     * Booleans are stored in intermediate storage as bytes (in the case of NDB). This allows us to convert
     * between the NDB-representation of a boolean and the Java representation.
     * @param val The byte value from NDB representing a boolean value.
     * @return The boolean value of the byte.
     */
    public static boolean convert(byte val) {
        if (val == TRUE) {
            return true;
        }
        if (val == FALSE) {
            return false;
        }
        throw new IllegalArgumentException();
    }

    /**
     * Print data from a received INV event.
     * @param invEventOp The event operation of the received INV event.
     * @param eventName The name of the event we received.
     * @param tableName The name of the table on which the event is registered.
     * @param timeReceived Instant object denoting the time at which we received this event.
     * @param preValues The NdbRecAttr instances for the pre-event values.
     * @param postvalues The NdbRecAttr instances for the post-event values.
     * @param eventColumns The names of the columns being monitored/reported on by the event.
     */
    private static void printInvalidationEvent(EventOperation invEventOp, String eventName, Instant timeReceived,
                                               String tableName, NdbRecAttr[] preValues, NdbRecAttr[] postvalues,
                                               String[] eventColumns) {
        System.out.println("=-=-=-=-=-=-=-=-=-=-=- INV RECEIVED =-=-=-=-=-=-=-=-=-=-=");
        System.out.println("Event Name: " + eventName + ", Table Name: " + tableName);
        System.out.println("Time Received: " + timeReceived);
        System.out.println(eventColumns[0] + ": " + postvalues[0].int64_value());
        System.out.println(eventColumns[1] + ": " + postvalues[1].int64_value());
        System.out.println(eventColumns[2] + ": " + postvalues[2].int64_value());
        System.out.println(eventColumns[3] + ": " + postvalues[3].int64_value());
        System.out.println(eventColumns[4] + ": " + postvalues[4].int64_value());
//        System.out.println(StringUtils.join(Arrays.asList(eventColumns), ", "));
//        System.out.printf(INV_EVENT_FORMAT, postvalues[0].int64_value(), postvalues[1].int64_value(),
//                postvalues[2].int64_value(), postvalues[3].int64_value(), postvalues[4].int64_value());
        System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");
    }

    /**
     * Print data from a received ACK event.
     * @param ackEventOp The event operation of the received ACK event.
     * @param eventName The name of the event we received.
     * @param tableName The name of the table on which the event is registered.
     * @param timeReceived Instant object denoting the time at which we received this event.
     * @param preValues The NdbRecAttr instances for the pre-event values.
     * @param postvalues The NdbRecAttr instances for the post-event values.
     * @param eventColumns The names of the columns being monitored/reported on by the event.
     */
    private static void printAcknowledgementEvent(EventOperation ackEventOp, String eventName, Instant timeReceived,
                                                  String tableName, NdbRecAttr[] preValues, NdbRecAttr[] postvalues,
                                                  String[] eventColumns) {
        System.out.println("=-=-=-=-=-=-=-=-=-=-=- ACK RECEIVED =-=-=-=-=-=-=-=-=-=-=");
        System.out.println("Event Name: " + eventName + ", Table Name: " + tableName);
        System.out.println("Time Received: " + timeReceived);
        System.out.println(eventColumns[0] + ": " + postvalues[0].int64_value());
        System.out.println(eventColumns[1] + ": " + postvalues[1].int32_value());
        System.out.println(eventColumns[2] + ": " + convert(postvalues[2].int8_value()));
        System.out.println(eventColumns[3] + ": " + postvalues[3].int64_value());
        System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("com.mysql.clusterj.connectstring", "10.241.64.15:1186");
        props.put("com.mysql.clusterj.database", "hop_bram_vm");

        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        session.createAndRegisterEvent("inv_table_watch0", "invalidations_deployment0",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT}, 0,
                true);

        session.createAndRegisterEvent("inv_table_watch1", "invalidations_deployment1",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT}, 0,
                true);

        session.createAndRegisterEvent("inv_table_watch2", "invalidations_deployment2",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT}, 0,
                true);

        session.createAndRegisterEvent("ack_table_watch0", "write_acks_deployment0",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.UPDATE}, 0,
                true);

        session.createAndRegisterEvent("ack_table_watch1", "write_acks_deployment1",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.UPDATE}, 0,
                true);

        session.createAndRegisterEvent("ack_table_watch2", "write_acks_deployment2",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.UPDATE}, 0,
                true);

        NdbEventOperationImpl ackEventOp0 = (NdbEventOperationImpl)session.createEventOperation("ack_table_watch0");
        NdbEventOperationImpl ackEventOp1 = (NdbEventOperationImpl)session.createEventOperation("ack_table_watch1");
        NdbEventOperationImpl ackEventOp2 = (NdbEventOperationImpl)session.createEventOperation("ack_table_watch2");

        NdbEventOperationImpl invEventOp0 = (NdbEventOperationImpl)session.createEventOperation("inv_table_watch0");
        NdbEventOperationImpl invEventOp1 = (NdbEventOperationImpl)session.createEventOperation("inv_table_watch1");
        NdbEventOperationImpl invEventOp2 = (NdbEventOperationImpl)session.createEventOperation("inv_table_watch2");

        NdbRecAttr[] event0PreNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
        NdbRecAttr[] event0PostNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
        NdbRecAttr[] event1PreNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
        NdbRecAttr[] event1PostNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
        NdbRecAttr[] event2PreNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
        NdbRecAttr[] event2PostNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];

        NdbRecAttr[] event0PreNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
        NdbRecAttr[] event0PostNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
        NdbRecAttr[] event1PreNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
        NdbRecAttr[] event1PostNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
        NdbRecAttr[] event2PreNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
        NdbRecAttr[] event2PostNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];

        String dbugString = "d:t:L:F:o,/home/ubuntu/repos/clusterj/dbug.log";
        Dbug dbug = ClusterJHelper.newDbug();
        dbug.push(dbugString);

        for (int i = 0; i < ACK_EVENT_COLUMNS.length; i++) {
            String eventColumnName = ACK_EVENT_COLUMNS[i];
            NdbRecAttr postAttr0 = ackEventOp0.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr0 = ackEventOp0.getNdbEventOperation().getPreValue(eventColumnName, null);
            event0PostNdbRecAttributes[i] = postAttr0;
            event0PreNdbRecAttributes[i] = preAttr0;

            NdbRecAttr postAttr1 = ackEventOp1.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr1 = ackEventOp1.getNdbEventOperation().getPreValue(eventColumnName, null);
            event1PostNdbRecAttributes[i] = postAttr1;
            event1PreNdbRecAttributes[i] = preAttr1;

            NdbRecAttr postAttr2 = ackEventOp2.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr2 = ackEventOp2.getNdbEventOperation().getPreValue(eventColumnName, null);
            event2PostNdbRecAttributes[i] = postAttr2;
            event2PreNdbRecAttributes[i] = preAttr2;
        }

        for (int i = 0; i < INV_TABLE_EVENT_COLUMNS.length; i++) {
            String eventColumnName = INV_TABLE_EVENT_COLUMNS[i];
            NdbRecAttr postAttr0 = invEventOp0.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr0 = invEventOp0.getNdbEventOperation().getPreValue(eventColumnName, null);
            event0PostNdbRecAttributesInv[i] = postAttr0;
            event0PreNdbRecAttributesInv[i] = preAttr0;

            NdbRecAttr postAttr1 = invEventOp1.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr1 = invEventOp1.getNdbEventOperation().getPreValue(eventColumnName, null);
            event1PostNdbRecAttributesInv[i] = postAttr1;
            event1PreNdbRecAttributesInv[i] = preAttr1;

            NdbRecAttr postAttr2 = invEventOp2.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr2 = invEventOp2.getNdbEventOperation().getPreValue(eventColumnName, null);
            event2PostNdbRecAttributesInv[i] = postAttr2;
            event2PreNdbRecAttributesInv[i] = preAttr2;
        }

        System.out.println("Executing ACK event operations.");
        ackEventOp0.execute();
        ackEventOp1.execute();
        ackEventOp2.execute();

        System.out.println("Executing INV event operations.");
        invEventOp0.execute();
        invEventOp1.execute();
        invEventOp2.execute();

        System.out.println("Listening for events now...");
        int eventCounter = 0;
        while (true) {
            boolean foundEvents = session.pollEvents(1000, null);

            if (!foundEvents) {
                continue;
            }

            Instant now = Instant.now();
            System.out.println("[" + now.toString() + "] Found events!");

            EventOperation nextEventOp = session.nextEvent();

            while (nextEventOp != null) {
                TableEvent eventType = nextEventOp.getEventType();

                now = Instant.now();
                System.out.println("\n\n[" + now.toString() + "] Event #" + eventCounter + ": " + eventType.name());

                NdbRecAttr[] postAttrs = null;
                NdbRecAttr[] preAttrs = null;
                String[] eventColumns = null;
                if (ackEventOp0.equals(nextEventOp)) {
                    now = Instant.now();
                    //System.out.println("[" + now.toString() + "] Received ACK Event for table write_acks_deployment0!");
                    postAttrs = event0PostNdbRecAttributes;
                    preAttrs = event0PreNdbRecAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                    printAcknowledgementEvent(nextEventOp, "ack_table_watch0", now,
                            "write_acks_deployment0", preAttrs, postAttrs, eventColumns);
                }
                else if (ackEventOp1.equals(nextEventOp)) {
                    now = Instant.now();
                    //System.out.println("[" + now.toString() + "] Received ACK Event for table write_acks_deployment1!");
                    postAttrs = event1PostNdbRecAttributes;
                    preAttrs = event1PreNdbRecAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                    printAcknowledgementEvent(nextEventOp, "ack_table_watch1", now,
                            "write_acks_deployment1", preAttrs, postAttrs, eventColumns);
                }
                else if (ackEventOp2.equals(nextEventOp)) {
                    now = Instant.now();
                    //System.out.println("[" + now.toString() + "] Received ACK Event for table write_acks_deployment2!");
                    postAttrs = event2PostNdbRecAttributes;
                    preAttrs = event2PreNdbRecAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                    printAcknowledgementEvent(nextEventOp, "ack_table_watch2", now,
                            "write_acks_deployment2", preAttrs, postAttrs, eventColumns);
                }
                else if (invEventOp0.equals(nextEventOp)) {
                    now = Instant.now();
                    //System.out.println("[" + now.toString() + "] Received INV Event for table inv_table_watch0!");
                    postAttrs = event0PostNdbRecAttributesInv;
                    preAttrs = event0PreNdbRecAttributesInv;
                    eventColumns = INV_TABLE_EVENT_COLUMNS;
                    printInvalidationEvent(nextEventOp, "inv_table_watch0", now,
                            "invalidations_deployment0", preAttrs, postAttrs, eventColumns);
                }
                else if (invEventOp1.equals(nextEventOp)) {
                    now = Instant.now();
                    //System.out.println("[" + now.toString() + "] Received INV Event for table inv_table_watch1!");
                    postAttrs = event1PostNdbRecAttributesInv;
                    preAttrs = event1PreNdbRecAttributesInv;
                    eventColumns = INV_TABLE_EVENT_COLUMNS;
                    printInvalidationEvent(nextEventOp, "inv_table_watch1", now,
                            "invalidations_deployment1", preAttrs, postAttrs, eventColumns);
                }
                else if (invEventOp2.equals(nextEventOp)) {
                    now = Instant.now();
                    //System.out.println("[" + now.toString() + "] Received INV Event for table inv_table_watch2!");
                    postAttrs = event2PostNdbRecAttributesInv;
                    preAttrs = event2PreNdbRecAttributesInv;
                    eventColumns = INV_TABLE_EVENT_COLUMNS;
                    printInvalidationEvent(nextEventOp, "inv_table_watch2", now,
                            "invalidations_deployment2", preAttrs, postAttrs, eventColumns);
                }
                else {
                    throw new IllegalStateException("Unknown event operation: " + nextEventOp);
                }
//                for (int i = 0; i < eventColumns.length; i++) {
//                    NdbRecAttr postAttr = postAttrs[i];
//                    NdbRecAttr preAttr = preAttrs[i];
//
//                    System.out.println("\t" + eventColumns[i] + " pre isNULL: " + preAttr.isNULL());
//                    System.out.println("\t" + eventColumns[i] + " post isNULL: " + postAttr.isNULL());
//
//                    System.out.println("\t" + eventColumns[i] + " pre size is " + preAttr.get_size_in_bytes() + " bytes");
//                    System.out.println("\t" + eventColumns[i] + " post size is " + postAttr.get_size_in_bytes() + " bytes");
//
//                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.int64_value());
//                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.int64_value());
//
//                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.int32_value());
//                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.int32_value());
//
//                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.int8_value());
//                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.int8_value());
//
//                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.u_8_value());
//                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.u_8_value());
//
//                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.u_char_value());
//                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.u_char_value());
//
//                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.char_value());
//                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.char_value() + "\n");
//                }
                nextEventOp = session.nextEvent();
                eventCounter++;
            }
        }
    }
}

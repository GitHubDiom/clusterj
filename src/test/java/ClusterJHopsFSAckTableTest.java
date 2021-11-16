import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.tie.NdbEventOperationImpl;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

public class ClusterJHopsFSAckTableTest {
    private static final String DEFAULT_CONNECT_STRING = "10.241.64.15:1186";
    private static final String DEFAULT_DATABASE = "hop_bram_vm";
    private static final String DEFAULT_TABLE_NAME = "write_acks_deployment0";
    private static final String DEFAULT_EVENT_NAME = "ack_table_watch0";

    private static final int DEFAULT_EVENT_LIMIT = 30;

    /**
     * Columns for which we want to see values for ACK events.
     */
    private static final String[] ACK_EVENT_COLUMNS = new String[] {
            WriteAcknowledgementsTableDef.NAME_NODE_ID,       // bigint(20)
            WriteAcknowledgementsTableDef.DEPLOYMENT_NUMBER,  // int(11)
            WriteAcknowledgementsTableDef.ACKNOWLEDGED,       // tinyint(4)
            WriteAcknowledgementsTableDef.OPERATION_ID        // bigint(20)
    };

    private static final String[] INV_TABLE_EVENT_COLUMNS = new String[] {
            InvalidationTablesDef.INODE_ID,        // bigint(20)
            InvalidationTablesDef.PARENT_ID,       // bigint(20)
            InvalidationTablesDef.LEADER_ID,       // bigint(20), so it's a long.
            InvalidationTablesDef.TX_START,        // bigint(20), so it's a long.
            InvalidationTablesDef.OPERATION_ID,    // bigint(20), so it's a long.
    };

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("com.mysql.clusterj.connectstring", "10.241.64.15:1186");
        props.put("com.mysql.clusterj.database", "hop_bram_vm");

        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        session.createAndRegisterEvent("inv_table_watch0", "invalidations_deployment0",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 0,
                true);

        session.createAndRegisterEvent("inv_table_watch1", "invalidations_deployment1",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 0,
                true);

        session.createAndRegisterEvent("inv_table_watch2", "invalidations_deployment2",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 0,
                true);

        session.createAndRegisterEvent("ack_table_watch0", "write_acks_deployment0",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 0,
                true);

        session.createAndRegisterEvent("ack_table_watch1", "write_acks_deployment1",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 0,
                true);

        session.createAndRegisterEvent("ack_table_watch2", "write_acks_deployment2",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 0,
                true);

        NdbEventOperationImpl eventOperation0 = (NdbEventOperationImpl)session.createEventOperation("ack_table_watch0");
//        NdbEventOperationImpl eventOperation1 = (NdbEventOperationImpl)session.createEventOperation("ack_table_watch1");
//        NdbEventOperationImpl eventOperation2 = (NdbEventOperationImpl)session.createEventOperation("ack_table_watch2");
//
//        NdbEventOperationImpl invEventOperation0 = (NdbEventOperationImpl)session.createEventOperation("inv_table_watch0");
//        NdbEventOperationImpl invEventOperation1 = (NdbEventOperationImpl)session.createEventOperation("inv_table_watch1");
//        NdbEventOperationImpl invEventOperation2 = (NdbEventOperationImpl)session.createEventOperation("inv_table_watch2");

        NdbRecAttr[] event0PreNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
        NdbRecAttr[] event0PostNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
//        NdbRecAttr[] event1PreNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
//        NdbRecAttr[] event1PostNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
//        NdbRecAttr[] event2PreNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
//        NdbRecAttr[] event2PostNdbRecAttributes = new NdbRecAttr[ACK_EVENT_COLUMNS.length];
//
//        NdbRecAttr[] event0PreNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
//        NdbRecAttr[] event0PostNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
//        NdbRecAttr[] event1PreNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
//        NdbRecAttr[] event1PostNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
//        NdbRecAttr[] event2PreNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];
//        NdbRecAttr[] event2PostNdbRecAttributesInv = new NdbRecAttr[INV_TABLE_EVENT_COLUMNS.length];

        String dbugString = "d:t:L:F";
        Dbug dbug = ClusterJHelper.newDbug();
        dbug.push(dbugString);

        for (int i = 0; i < ACK_EVENT_COLUMNS.length; i++) {
            String eventColumnName = ACK_EVENT_COLUMNS[i];
            NdbRecAttr postAttr0 = eventOperation0.getNdbEventOperation().getValue(eventColumnName, null);
            NdbRecAttr preAttr0 = eventOperation0.getNdbEventOperation().getPreValue(eventColumnName, null);
            event0PostNdbRecAttributes[i] = postAttr0;
            event0PreNdbRecAttributes[i] = preAttr0;

//            NdbRecAttr postAttr1 = eventOperation1.getNdbEventOperation().getValue(eventColumnName, null);
//            NdbRecAttr preAttr1 = eventOperation1.getNdbEventOperation().getPreValue(eventColumnName, null);
//            event1PostNdbRecAttributes[i] = postAttr1;
//            event1PreNdbRecAttributes[i] = preAttr1;
//
//            NdbRecAttr postAttr2 = eventOperation2.getNdbEventOperation().getValue(eventColumnName, null);
//            NdbRecAttr preAttr2 = eventOperation2.getNdbEventOperation().getPreValue(eventColumnName, null);
//            event2PostNdbRecAttributes[i] = postAttr2;
//            event2PreNdbRecAttributes[i] = preAttr2;
        }

//        for (int i = 0; i < INV_TABLE_EVENT_COLUMNS.length; i++) {
//            String eventColumnName = INV_TABLE_EVENT_COLUMNS[i];
//            NdbRecAttr postAttr0 = invEventOperation0.getNdbEventOperation().getValue(eventColumnName, null);
//            NdbRecAttr preAttr0 = invEventOperation0.getNdbEventOperation().getPreValue(eventColumnName, null);
//            event0PostNdbRecAttributesInv[i] = postAttr0;
//            event0PreNdbRecAttributesInv[i] = preAttr0;
//
//            NdbRecAttr postAttr1 = invEventOperation1.getNdbEventOperation().getValue(eventColumnName, null);
//            NdbRecAttr preAttr1 = invEventOperation1.getNdbEventOperation().getPreValue(eventColumnName, null);
//            event1PostNdbRecAttributesInv[i] = postAttr1;
//            event1PreNdbRecAttributesInv[i] = preAttr1;
//
//            NdbRecAttr postAttr2 = invEventOperation2.getNdbEventOperation().getValue(eventColumnName, null);
//            NdbRecAttr preAttr2 = invEventOperation2.getNdbEventOperation().getPreValue(eventColumnName, null);
//            event2PostNdbRecAttributesInv[i] = postAttr2;
//            event2PreNdbRecAttributesInv[i] = preAttr2;
//        }

        System.out.println("Executing ACK event operations.");
        eventOperation0.execute();
//        eventOperation1.execute();
//        eventOperation2.execute();

//        System.out.println("Executing INV event operations.");
//        invEventOperation0.execute();
//        invEventOperation1.execute();
//        invEventOperation2.execute();

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
                if (eventOperation0.equals(nextEventOp)) {
                    now = Instant.now();
                    System.out.println("[" + now.toString() + "] Received ACK Event for table write_acks_deployment0!");
                    postAttrs = event0PostNdbRecAttributes;
                    preAttrs = event0PreNdbRecAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                }
//                else if (eventOperation1.equals(nextEventOp)) {
//                    now = Instant.now();
//                    System.out.println("[" + now.toString() + "] Received ACK Event for table write_acks_deployment1!");
//                    postAttrs = event1PostNdbRecAttributes;
//                    preAttrs = event1PreNdbRecAttributes;
//                    eventColumns = ACK_EVENT_COLUMNS;
//                }
//                else if (eventOperation2.equals(nextEventOp)) {
//                    now = Instant.now();
//                    System.out.println("[" + now.toString() + "] Received ACK Event for table write_acks_deployment2!");
//                    postAttrs = event2PostNdbRecAttributes;
//                    preAttrs = event2PreNdbRecAttributes;
//                    eventColumns = ACK_EVENT_COLUMNS;
//                }
//                else if (invEventOperation0.equals(nextEventOp)) {
//                    now = Instant.now();
//                    System.out.println("[" + now.toString() + "] Received INV Event for table inv_table_watch0!");
//                    postAttrs = event0PostNdbRecAttributesInv;
//                    preAttrs = event0PreNdbRecAttributesInv;
//                    eventColumns = INV_TABLE_EVENT_COLUMNS;
//                }
//                else if (invEventOperation1.equals(nextEventOp)) {
//                    now = Instant.now();
//                    System.out.println("[" + now.toString() + "] Received INV Event for table inv_table_watch1!");
//                    postAttrs = event1PostNdbRecAttributesInv;
//                    preAttrs = event1PreNdbRecAttributesInv;
//                    eventColumns = INV_TABLE_EVENT_COLUMNS;
//                }
//                else if (invEventOperation2.equals(nextEventOp)) {
//                    now = Instant.now();
//                    System.out.println("[" + now.toString() + "] Received INV Event for table inv_table_watch2!");
//                    postAttrs = event2PostNdbRecAttributesInv;
//                    preAttrs = event2PreNdbRecAttributesInv;
//                    eventColumns = INV_TABLE_EVENT_COLUMNS;
//                }
                else {
                    throw new IllegalStateException("Unknown event operation: " + nextEventOp);
                }

                for (int i = 0; i < eventColumns.length; i++) {
                    NdbRecAttr postAttr = postAttrs[i];
                    NdbRecAttr preAttr = preAttrs[i];

                    System.out.println("\t" + eventColumns[i] + " pre isNULL: " + preAttr.isNULL());
                    System.out.println("\t" + eventColumns[i] + " post isNULL: " + postAttr.isNULL());

                    System.out.println("\t" + eventColumns[i] + " pre size is " + preAttr.get_size_in_bytes() + " bytes");
                    System.out.println("\t" + eventColumns[i] + " post size is " + postAttr.get_size_in_bytes() + " bytes");

                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.int64_value());
                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.int64_value());

                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.int32_value());
                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.int32_value());

                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.int8_value());
                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.int8_value());

                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.u_8_value());
                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.u_8_value());

                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.u_char_value());
                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.u_char_value());

                    System.out.println("\t" + eventColumns[i] + " pre: " + preAttr.char_value());
                    System.out.println("\t" + eventColumns[i] + " post: " + postAttr.char_value() + "\n");
                }

                nextEventOp = session.nextEvent();
                eventCounter++;
            }
        }
    }
}

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;

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
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 1,
                true);

        session.createAndRegisterEvent("inv_table_watch1", "invalidations_deployment1",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 1,
                true);

        session.createAndRegisterEvent("inv_table_watch2", "invalidations_deployment2",
                INV_TABLE_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 1,
                true);

        session.createAndRegisterEvent("ack_table_watch0", "write_acks_deployment0",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 1,
                true);

        session.createAndRegisterEvent("ack_table_watch1", "write_acks_deployment1",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 1,
                true);

        session.createAndRegisterEvent("ack_table_watch2", "write_acks_deployment2",
                ACK_EVENT_COLUMNS, new TableEvent[]{TableEvent.INSERT, TableEvent.UPDATE, TableEvent.DELETE}, 1,
                true);

        EventOperation eventOperation0 = session.createEventOperation("ack_table_watch0");
        EventOperation eventOperation1 = session.createEventOperation("ack_table_watch1");
        EventOperation eventOperation2 = session.createEventOperation("ack_table_watch2");

        EventOperation invEventOperation0 = session.createEventOperation("inv_table_watch0");
        EventOperation invEventOperation1 = session.createEventOperation("inv_table_watch1");
        EventOperation invEventOperation2 = session.createEventOperation("inv_table_watch2");

        RecordAttr[] event0PreRecordAttributes = new RecordAttr[ACK_EVENT_COLUMNS.length];
        RecordAttr[] event0PostRecordAttributes = new RecordAttr[ACK_EVENT_COLUMNS.length];
        RecordAttr[] event1PreRecordAttributes = new RecordAttr[ACK_EVENT_COLUMNS.length];
        RecordAttr[] event1PostRecordAttributes = new RecordAttr[ACK_EVENT_COLUMNS.length];
        RecordAttr[] event2PreRecordAttributes = new RecordAttr[ACK_EVENT_COLUMNS.length];
        RecordAttr[] event2PostRecordAttributes = new RecordAttr[ACK_EVENT_COLUMNS.length];

        RecordAttr[] event0PreRecordAttributesInv = new RecordAttr[INV_TABLE_EVENT_COLUMNS.length];
        RecordAttr[] event0PostRecordAttributesInv = new RecordAttr[INV_TABLE_EVENT_COLUMNS.length];
        RecordAttr[] event1PreRecordAttributesInv = new RecordAttr[INV_TABLE_EVENT_COLUMNS.length];
        RecordAttr[] event1PostRecordAttributesInv = new RecordAttr[INV_TABLE_EVENT_COLUMNS.length];
        RecordAttr[] event2PreRecordAttributesInv = new RecordAttr[INV_TABLE_EVENT_COLUMNS.length];
        RecordAttr[] event2PostRecordAttributesInv = new RecordAttr[INV_TABLE_EVENT_COLUMNS.length];

        for (int i = 0; i < ACK_EVENT_COLUMNS.length; i++) {
            String eventColumnName = ACK_EVENT_COLUMNS[i];
            RecordAttr postAttr0 = eventOperation0.getValue(eventColumnName);
            RecordAttr preAttr0 = eventOperation0.getPreValue(eventColumnName);
            event0PostRecordAttributes[i] = postAttr0;
            event0PreRecordAttributes[i] = preAttr0;

            RecordAttr postAttr1 = eventOperation1.getValue(eventColumnName);
            RecordAttr preAttr1 = eventOperation1.getPreValue(eventColumnName);
            event1PostRecordAttributes[i] = postAttr1;
            event1PreRecordAttributes[i] = preAttr1;

            RecordAttr postAttr2 = eventOperation2.getValue(eventColumnName);
            RecordAttr preAttr2 = eventOperation2.getPreValue(eventColumnName);
            event2PostRecordAttributes[i] = postAttr2;
            event2PreRecordAttributes[i] = preAttr2;
        }

        for (int i = 0; i < INV_TABLE_EVENT_COLUMNS.length; i++) {
            String eventColumnName = INV_TABLE_EVENT_COLUMNS[i];
            RecordAttr postAttr0 = invEventOperation0.getValue(eventColumnName);
            RecordAttr preAttr0 = invEventOperation0.getPreValue(eventColumnName);
            event0PostRecordAttributesInv[i] = postAttr0;
            event0PreRecordAttributesInv[i] = preAttr0;

            RecordAttr postAttr1 = invEventOperation1.getValue(eventColumnName);
            RecordAttr preAttr1 = invEventOperation1.getPreValue(eventColumnName);
            event1PostRecordAttributesInv[i] = postAttr1;
            event1PreRecordAttributesInv[i] = preAttr1;

            RecordAttr postAttr2 = invEventOperation2.getValue(eventColumnName);
            RecordAttr preAttr2 = invEventOperation2.getPreValue(eventColumnName);
            event2PostRecordAttributesInv[i] = postAttr2;
            event2PreRecordAttributesInv[i] = preAttr2;
        }

        System.out.println("Executing ACK event operations.");
        eventOperation0.execute();
        eventOperation1.execute();
        eventOperation2.execute();

        System.out.println("Executing INV event operations.");
        invEventOperation0.execute();
        invEventOperation1.execute();
        invEventOperation2.execute();

        System.out.println("Listening for events now...");
        int eventCounter = 0;
        while (true) {
            boolean foundEvents = session.pollEvents(1000, null);

            if (!foundEvents) {
                continue;
            }

            System.out.println("Found events!");

            EventOperation nextEventOp = session.nextEvent();

            while (nextEventOp != null) {
                TableEvent eventType = nextEventOp.getEventType();

                System.out.println("Event #" + eventCounter + ": " + eventType.name());

                RecordAttr[] postAttrs = null;
                RecordAttr[] preAttrs = null;
                String[] eventColumns = null;
                if (eventOperation0.equals(nextEventOp)) {
                    System.out.println("\nReceived ACK Event for table write_acks_deployment0!");
                    postAttrs = event0PostRecordAttributes;
                    preAttrs = event0PreRecordAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                }
                else if (eventOperation1.equals(nextEventOp)) {
                    System.out.println("\nReceived ACK Event for table write_acks_deployment1!");
                    postAttrs = event1PostRecordAttributes;
                    preAttrs = event1PreRecordAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                }
                else if (eventOperation2.equals(nextEventOp)) {
                    System.out.println("\nReceived ACK Event for table write_acks_deployment2!");
                    postAttrs = event2PostRecordAttributes;
                    preAttrs = event2PreRecordAttributes;
                    eventColumns = ACK_EVENT_COLUMNS;
                }
                else if (invEventOperation0.equals(nextEventOp)) {
                    System.out.println("\nReceived INV Event for table inv_table_watch0!");
                    postAttrs = event0PostRecordAttributesInv;
                    preAttrs = event0PreRecordAttributesInv;
                    eventColumns = INV_TABLE_EVENT_COLUMNS;
                }
                else if (invEventOperation1.equals(nextEventOp)) {
                    System.out.println("\nReceived INV Event for table inv_table_watch1!");
                    postAttrs = event2PostRecordAttributesInv;
                    preAttrs = event2PreRecordAttributesInv;
                    eventColumns = INV_TABLE_EVENT_COLUMNS;
                }
                else if (invEventOperation2.equals(nextEventOp)) {
                    System.out.println("\nReceived INV Event for table inv_table_watch2!");
                    postAttrs = event2PostRecordAttributesInv;
                    preAttrs = event2PreRecordAttributesInv;
                    eventColumns = INV_TABLE_EVENT_COLUMNS;
                }
                else {
                    throw new IllegalStateException("Unknown event operation: " + nextEventOp);
                }

                for (int i = 0; i < eventColumns.length; i++) {
                    RecordAttr postAttr = postAttrs[i];
                    RecordAttr preAttr = preAttrs[i];

                    // First two columns are integers, second two are strings.
                    if (i < 2) {
                        System.out.println("Pre: " + preAttr.u_32_value());
                        System.out.println("Post: " + postAttr.u_32_value());
                    } else {
                        System.out.println("Pre: " + preAttr.toString());
                        System.out.println("Post: " + postAttr.toString());
                    }
                }

                nextEventOp = session.nextEvent();
                eventCounter++;
            }
        }
    }
}

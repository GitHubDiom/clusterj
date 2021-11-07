import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class ClusterJMultipleEventTest {
    private static final String DEFAULT_CONNECT_STRING = "10.241.64.15:1186";
    private static final String DEFAULT_DATABASE = "ndb_examples";
    private static final String DEFAULT_TABLE_NAME_EVENT1 = "t0";
    private static final String DEFAULT_TABLE_NAME_EVENT2 = "t1";
    private static final String DEFAULT_EVENT_NAME1 = "MY_EVENT1_t0";
    private static final String DEFAULT_EVENT_NAME2 = "MY_EVENT2_t1";
    private static final String DEFAULT_DEBUG_STRING = "d:t:L:F:o,/home/ubuntu/repos/clusterj/dbug.log";

    private static final int DEFAULT_EVENT_LIMIT = 30;

    private static final String[] t0Columns = new String[] {"c0", "c1", "c2", "c3", "c4" };

    private static final String[] t1Columns = new String[] {"m0", "m1", "m2", "m3", "m4" };

    public static final String[] datanodesColumns = new String[] {
            "datanode_uuid",
            "hostname",
            "ipaddr",
            "xfer_port",
            "info_port",
            "info_secure_port",
            "ipc_port",
            "creation_time"
    };

    private static final HashMap<String, String[]> eventColumnNames = new HashMap<>();

    public static void main(String[] args) throws Exception {
        eventColumnNames.put("t0", t0Columns);
        eventColumnNames.put("t1", t1Columns);
        eventColumnNames.put("datanodes", datanodesColumns);

        Options options = new Options();

        Option connectStringOption = new Option(
                "c", "connect_string", true,
                "The MySQL NDB connection string. Default: " +  DEFAULT_CONNECT_STRING
        );

        Option databaseOption = new Option(
                "d", "database", true,
                "The MySQL database to use. Default: " + DEFAULT_DATABASE
        );

        Option tableNameOption1 = new Option(
                "t1", "table_name1", true,
                "Name of the table on which the Event will be created. Default: " + DEFAULT_TABLE_NAME_EVENT1
        );

        Option tableNameOption2 = new Option(
                "t2", "table_name2", true,
                "Name of the table on which the Event will be created. Default: " + DEFAULT_TABLE_NAME_EVENT2
        );

        Option eventName1Option = new Option(
                "e1", "event_name1", true,
                "What to name the first event. Default: " + DEFAULT_EVENT_NAME1
        );

        Option eventName2Option = new Option(
                "e2", "event_name2", true,
                "What to name the second event. Default: " + DEFAULT_EVENT_NAME2
        );

        Option timeoutOption = new Option(
                "l", "event_limit", true,
                "Number of events to listen for before stopping. Default: " + DEFAULT_EVENT_LIMIT
        );

        Option forceOption = new Option(
                "f", "force", true,
                "Pass '1' for the force argument to dropEvent(), if a call to that function occurs." +
                        " Default: 0."
        );

        Option debugStringOption = new Option(
                "ds", "debug_string", true,
                "Debug string to pass to underlying NDB API. " +
                        "Default is: \"d:t:L:F:o,/home/ubuntu/repos/clusterj/dbug.log\""
        );

        Option deleteIfExistsOption = new Option(
                "del", "delete_if_exists", false,
                "If passed, then delete the event and recreate it if it already exists. If not passed," +
                        "then this will simply try to use the existing event if it discovers it."
        );

//        Option eventColumnSet = new Option(
//                "n", "event_col_name_set", true,
//                "Selects the array of event column names to use. 0 for c0, c1, c2, c3, c4. 1 for m0, m1, " +
//                        "m2, m3, m4"
//        );

        options.addOption(connectStringOption);
        options.addOption(databaseOption);
        options.addOption(tableNameOption1);
        options.addOption(tableNameOption2);
        options.addOption(eventName1Option);
        options.addOption(eventName2Option);
        options.addOption(timeoutOption);
        options.addOption(forceOption);
        options.addOption(debugStringOption);
        options.addOption(deleteIfExistsOption);
//        options.addOption(eventColumnSet);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        String connectString = DEFAULT_CONNECT_STRING;
        String database = DEFAULT_DATABASE;
        String tableName1 = DEFAULT_TABLE_NAME_EVENT1;
        String tableName2 = DEFAULT_TABLE_NAME_EVENT2;
        String eventName1 = DEFAULT_EVENT_NAME1;
        String eventName2 = DEFAULT_EVENT_NAME2;
        String debugString = DEFAULT_DEBUG_STRING;
        int eventLimit = DEFAULT_EVENT_LIMIT;
        int force = 0;
//        int eventColumnNameSet = 0;

        if (cmd.hasOption("connect_string"))
            connectString = cmd.getOptionValue("connect_string");

        if (cmd.hasOption("database"))
            database = cmd.getOptionValue("database");

        if (cmd.hasOption("event_name1"))
            eventName1 = cmd.getOptionValue("event_name1");

        if (cmd.hasOption("event_name2"))
            eventName2 = cmd.getOptionValue("event_name2");

        if (cmd.hasOption("table_name1"))
            tableName1 = cmd.getOptionValue("table_name1");

        if (cmd.hasOption("table_name2"))
            tableName2 = cmd.getOptionValue("table_name2");

        if (cmd.hasOption("event_limit"))
            eventLimit = Integer.parseInt(cmd.getOptionValue("event_limit"));

        if (cmd.hasOption("force"))
            force = Integer.parseInt(cmd.getOptionValue("force"));

        if (cmd.hasOption("debug_string"))
            debugString = cmd.getOptionValue("debug_string");

        boolean deleteIfExists = cmd.hasOption("delete_if_exists");

//        if (cmd.hasOption("event_col_name_set"))
//            eventColumnNameSet = Integer.parseInt(cmd.getOptionValue("event_col_name_set"));

        Dbug dbug = ClusterJHelper.newDbug();

        // Pause execution.
        System.out.println("Assigning debug string \"" + debugString + "\" now...");

        dbug.push(debugString);
        String newDbug = dbug.get();

        System.out.println("New debug string: \"" + newDbug + "\"");

        Properties props = new Properties();
        props.put("com.mysql.clusterj.connectstring", connectString);
        props.put("com.mysql.clusterj.database", database);

        System.out.println("\n=-=-=-=-=-=-=-=-=-=-= Arguments Debug Information =-=-=-=-=-=-=-=-=-=-=");
        System.out.println("Using connect string \"" + connectString + "\" to connect to NDB cluster.");
        System.out.println("Target database: " + database);
        System.out.println("Target table for event1: " + tableName1);
        System.out.println("Target table for event2: " + tableName2);
        System.out.println("Event1 name: " + eventName1);
        System.out.println("Event2 name: " + eventName2);
        System.out.println("Event limit: " + eventLimit);
        System.out.println("Force: " + force);
        System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");

        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        String[] colNames1 = eventColumnNames.get(tableName1);
        String[] colNames2 = eventColumnNames.get(tableName2);

//        if (eventColumnNameSet == 0)
//            eventColumnNames = eventColumnNames1;
//        else if (eventColumnNameSet == 1)
//            eventColumnNames = eventColumnNames2;
//        else
//            throw new IllegalArgumentException("Invalid EventColumnName set specified: " + eventColumnNameSet);

        System.out.println("Event1 column names: " + Arrays.toString(colNames1));
        System.out.println("Event2 column names: " + Arrays.toString(colNames2));

        System.out.println("Checking to see if event with name " + eventName1 + " already exists...");
        Event event1 = session.getEvent(eventName1);

        System.out.println("Event " + eventName1 + " already exists: " + (event1 != null));
        boolean event1AlreadyExists = false;
        if (event1 != null) {
            System.out.println("Event " + eventName1 + ": " + event1);
            event1AlreadyExists = true;
        }

        System.out.println("Checking to see if event with name " + eventName2 + " already exists...");
        Event event2 = session.getEvent(eventName2);

        System.out.println("Event " + eventName2 + " already exists: " + (event2 != null));
        boolean event2AlreadyExists = false;
        if (event2 != null) {
            System.out.println("Event " + eventName2 + ": " + event2);
            event2AlreadyExists = true;
        }

        // If the event either:
        //      (1) does not already exist
        //      (2) does already exist AND we're supposed to delete and (re)create it
        // then go ahead and create and register the event (which will delete and recreate it if necessary)
        if (!event1AlreadyExists || deleteIfExists) {
            session.createAndRegisterEvent(
                    eventName1,
                    tableName1,
                    colNames1,
                    new TableEvent[] { TableEvent.ALL },
                    force,
                    true);
        } else {
            System.out.println("Will re-use existing event " + eventName1 + ".");
        }

        if (!event2AlreadyExists || deleteIfExists) {
            session.createAndRegisterEvent(
                    eventName2,
                    tableName2,
                    colNames2,
                    new TableEvent[] { TableEvent.ALL },
                    force,
                    true);
        } else {
            System.out.println("Will re-use existing event " + eventName2 + ".");
        }

        EventOperation event1OperationOriginal = session.createEventOperation(eventName1);
        EventOperation event2OperationOriginal = session.createEventOperation(eventName2);

        RecordAttr[] postAttrsEvent1 = new RecordAttr[colNames1.length];
        RecordAttr[] preAttrsEvent1 = new RecordAttr[colNames1.length];

        System.out.println("Creating/retrieving record attributes for the event1 columns now...");
        for (int i = 0; i < colNames1.length; i++) {
            System.out.println("\tCreating/retrieving attributes for column " + (i + 1) + "/" + colNames1.length);
            String eventColumnName = colNames1[i];
            RecordAttr postAttr = event1OperationOriginal.getValue(eventColumnName);

            System.out.println("\tSuccessfully retrieved post-value record attribute for column " + eventColumnName);

            RecordAttr preAttr = event1OperationOriginal.getPreValue(eventColumnName);

            System.out.println("\tSuccessfully retrieved pre-value record attribute for column " + eventColumnName);

            postAttrsEvent1[i] = postAttr;
            preAttrsEvent1[i] = preAttr;
        }

        RecordAttr[] postAttrsEvent2 = new RecordAttr[colNames2.length];
        RecordAttr[] preAttrsEvent2 = new RecordAttr[colNames2.length];

        System.out.println("Creating/retrieving record attributes for the event2 columns now...");
        for (int i = 0; i < colNames2.length; i++) {
            System.out.println("\tCreating/retrieving attributes for column " + (i + 1) + "/" + colNames2.length);
            String eventColumnName = colNames2[i];
            RecordAttr postAttr = event1OperationOriginal.getValue(eventColumnName);

            System.out.println("\tSuccessfully retrieved post-value record attribute for column " + eventColumnName);

            RecordAttr preAttr = event1OperationOriginal.getPreValue(eventColumnName);

            System.out.println("\tSuccessfully retrieved pre-value record attribute for column " + eventColumnName);

            postAttrsEvent2[i] = postAttr;
            preAttrsEvent2[i] = preAttr;
        }

        System.out.println("Executing Event Operation for event " + eventName1 + " now...");
        event1OperationOriginal.execute();

        System.out.println("Executing Event Operation for event " + eventName2 + " now...");
        event2OperationOriginal.execute();

        System.out.println("Polling until a total of " + eventLimit + " events are received now...");
        int eventCounter = 0;
        while (eventCounter < eventLimit) {
            boolean foundEvents = session.pollEvents(1000, null);

            if (!foundEvents) {
                continue;
            }

            EventOperation nextEventOp = session.nextEvent();

            System.out.println("Initial return value of nextEvent(): " + nextEventOp.toString());

            System.out.println("(event1OperationOriginal == nextEventOp): " + (event1OperationOriginal == nextEventOp));
            System.out.println("(event2OperationOriginal == nextEventOp): " + (event2OperationOriginal == nextEventOp));

            while (nextEventOp != null) {
                TableEvent eventType = nextEventOp.getEventType();

                System.out.println("Event #" + eventCounter + ": " + eventType.name());

                String[] colNames;
                if (event1OperationOriginal == nextEventOp)
                    colNames = colNames1;
                else if (event2OperationOriginal == nextEventOp)
                    colNames = colNames2;
                else
                    throw new Exception("Unable to determine which event this corresponds to...");

                RecordAttr[] postAttrs = (event1OperationOriginal == nextEventOp) ? postAttrsEvent1 : postAttrsEvent2;
                RecordAttr[] preAttrs = (event1OperationOriginal == nextEventOp) ? preAttrsEvent1 : preAttrsEvent2;

                for (int i = 0; i < colNames.length; i++) {
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
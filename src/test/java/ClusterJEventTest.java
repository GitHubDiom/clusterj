import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import org.apache.commons.cli.*;

import java.util.Properties;

public class ClusterJEventTest {

    private static final String DEFAULT_CONNECT_STRING = "10.241.64.15:1186";
    private static final String DEFAULT_DATABASE = "ndb_examples";
    private static final String DEFAULT_TABLE_NAME = "t0";
    private static final String DEFAULT_EVENT_NAME = "MY_EVENT_t0";
    private static final String DEFAULT_DEBUG_STRING = "d:t:L:F:o,/home/ubuntu/repos/clusterj/dbug.log";

    private static final int DEFAULT_TIMEOUT = 30;

    public static void main(String[] args) {
        Options options = new Options();

        Option connectStringOption = new Option(
                "c", "connect_string", true,
                "The MySQL NDB connection string. Default: " +  DEFAULT_CONNECT_STRING
        );

        Option databaseOption = new Option(
                "d", "database", true,
                "The MySQL database to use. Default: " + DEFAULT_DATABASE
        );

        Option tableNameOption = new Option(
                "n", "table_name", true,
                "Name of the table on which the Event will be created. Default: " + DEFAULT_TABLE_NAME
        );

        Option eventNameOption = new Option(
                "e", "event_name", true,
                "What to name the event. Default: " + DEFAULT_EVENT_NAME
        );

        Option timeoutOption = new Option(
                "t", "timeout", true,
                "Number of events to listen for before stopping. Default: " + DEFAULT_TIMEOUT
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

        options.addOption(connectStringOption);
        options.addOption(databaseOption);
        options.addOption(tableNameOption);
        options.addOption(eventNameOption);
        options.addOption(timeoutOption);
        options.addOption(forceOption);
        options.addOption(debugStringOption);
        options.addOption(deleteIfExistsOption);

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
        String tableName = DEFAULT_TABLE_NAME;
        String eventName = DEFAULT_EVENT_NAME;
        String debugString = DEFAULT_DEBUG_STRING;
        int timeout = DEFAULT_TIMEOUT;
        int force = 0;

        if (cmd.hasOption("connect_string"))
            connectString = cmd.getOptionValue("connect_string");

        if (cmd.hasOption("database"))
            database = cmd.getOptionValue("database");

        if (cmd.hasOption("event_name"))
            eventName = cmd.getOptionValue("event_name");

        if (cmd.hasOption("table_name"))
            tableName = cmd.getOptionValue("table_name");

        if (cmd.hasOption("timeout"))
            timeout = Integer.parseInt(cmd.getOptionValue("timeout"));

        if (cmd.hasOption("force"))
            force = Integer.parseInt(cmd.getOptionValue("force"));

        if (cmd.hasOption("debug_string"))
            debugString = cmd.getOptionValue("debug_string");

        boolean deleteIfExists = false;
        if (cmd.hasOption("delete_if_exists"))
            deleteIfExists = true;

        if (cmd.hasOption("suppressClusterJDebugOption")) {

        }

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
        System.out.println("Target table: " + tableName);
        System.out.println("Event name: " + eventName);
        System.out.println("Timeout: " + timeout);
        System.out.println("Force: " + force);
        System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");

        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        String[] eventColumnNames = new String[] {
                "c0",
                "c1",
                "c2",
                "c3",
                "c4"
        };

        System.out.println("Checking to see if event with name " + eventName + " already exists...");
        Event event = session.getEvent(eventName);

        System.out.println("Event " + eventName + " already exists: " + (event != null));
        boolean alreadyExists = false;
        if (event != null) {
            System.out.println("Event " + eventName + ": " + event);
            alreadyExists = true;
        }

        // If the event either:
        //      (1) does not already exist
        //      (2) does already exist AND we're supposed to delete and (re)create it
        // then go ahead and create and register the event (which will delete and recreate it if necessary)
        if (!alreadyExists || (alreadyExists && deleteIfExists)) {
            session.createAndRegisterEvent(
                    eventName,
                    tableName,
                    eventColumnNames,
                    new TableEvent[] { TableEvent.ALL },
                    force);
        } else {
            System.out.println("Will re-use existing event " + eventName + ".");
        }

        EventOperation eventOperation = session.createEventOperation(eventName);

        RecordAttr[] postAttrs = new RecordAttr[eventColumnNames.length];
        RecordAttr[] preAttrs = new RecordAttr[eventColumnNames.length];

        System.out.println("Creating/retrieving record attributes for the event columns now...");
        for (int i = 0; i < eventColumnNames.length; i++) {
            System.out.println("\tCreating/retrieving attributes for column " + (i + 1) + "/" + eventColumnNames.length);
            String eventColumnName = eventColumnNames[i];
            RecordAttr postAttr = eventOperation.getValue(eventColumnName);

            System.out.println("\tSuccessfully retrieved post-value record attribute for column " + eventColumnName);

            RecordAttr preAttr = eventOperation.getPreValue(eventColumnName);

            System.out.println("\tSuccessfully retrieved pre-value record attribute for column " + eventColumnName);

            postAttrs[i] = postAttr;
            preAttrs[i] = preAttr;
        }

        System.out.println("Executing Event Operation for event " + eventName + " now...");
        eventOperation.execute();

        System.out.println("Polling for a maximum of " + timeout + " events now...");
        int eventCounter = 0;
        while (eventCounter < timeout) {
            boolean foundEvents = session.pollEvents(1000, null);

            if (!foundEvents) {
                continue;
            }

            EventOperation nextEventOp = session.nextEvent();

            System.out.println("Initial return value of nextEvent(): " + nextEventOp.toString());

            while (nextEventOp != null) {
                TableEvent eventType = nextEventOp.getEventType();

                System.out.println("Event #" + eventCounter + ": " + eventType.name());

                for (int i = 0; i < eventColumnNames.length; i++) {
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

import com.mysql.clusterj.*;
import com.mysql.clusterj.core.store.Event;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import org.apache.commons.cli.*;

import java.util.List;
import java.util.Properties;

public class ClusterJCreateDeleteTest {

    private static final String DEFAULT_CONNECT_STRING = "10.241.64.15:1186";
    private static final String DEFAULT_DATABASE = "ndb_examples";
    private static final String DEFAULT_TABLE_NAME = "t0";
    private static final String DEFAULT_EVENT_NAME = "MY_EVENT_t0";
    private static final String DEFAULT_DEBUG_STRING = "d:t:L:F:o,/home/ubuntu/repos/clusterj/dbug.log";
    private static final String DEFAULT_OPERATION = "create";

    private static final int DEFAULT_TIMEOUT = 30;

    public static void main(String[] args) {
        Options options = new Options();

        Option connectStringOption = new Option(
                "c", "connect_string", true,
                "The MySQL NDB connection string. Default: " + DEFAULT_CONNECT_STRING
        );

        Option databaseOption = new Option(
                "d", "database", true,
                "The MySQL database to use. Default: " + DEFAULT_DATABASE
        );

        Option operationOption = new Option(
                "o", "operation", true,
                "Specify the operation to perform. Options are \"create\", \"delete\", \"list\", \"get\", " +
                        "Default: \"create\""
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
                "Only used when dropping an event. If 1, then the drop is attempted without checking if " +
                        " the event exists first. If 0, then checks if event exists before trying to drop it. " +
                        " Default: 0."
        );

        Option debugStringOption = new Option(
                "ds", "debug_string", true,
                "Debug string to pass to underlying NDB API. " +
                        "Default is: \"d:t:L:F:o,/home/ubuntu/repos/clusterj/dbug.log\""
        );

        options.addOption(connectStringOption);
        options.addOption(databaseOption);
        options.addOption(tableNameOption);
        options.addOption(eventNameOption);
        options.addOption(timeoutOption);
        options.addOption(forceOption);
        options.addOption(debugStringOption);
        options.addOption(operationOption);

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
        String operation = DEFAULT_OPERATION;
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

        if (cmd.hasOption("operation"))
            operation = cmd.getOptionValue("operation");

        // assert(operation.equals("create") || operation.equals("delete") || operation.equals("c") || operation.equals("d"));

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

        if (operation.equals("delete") || operation.equals("drop") || operation.equals("d")) {
            boolean dropped = session.dropEvent(eventName, force);

            System.out.println("Event " + eventName + " dropped: " + dropped);
        }
        else if (operation.equals("create") || operation.equals("add") || operation.equals("c")) {
            String[] eventColumnNames = new String[]{
                    "c0",
                    "c1",
                    "c2",
                    "c3",
                    "c4"
            };

            session.createAndRegisterEvent(
                    eventName, tableName, eventColumnNames, new TableEvent[] {TableEvent.ALL},0);
        }
        else if (operation.equals("list") || operation.equals("l")) {
            List<String> eventNames = session.getEventNames();

            System.out.println("Events: ");
            for(int i = 0; i < eventNames.size(); i++) {
                System.out.println("\t #" + i + ": " + eventNames.get(i));
            }
        }
        else if (operation.equals("get") || operation.equals("g")) {
            Event event = session.getEvent(eventName);

            if (event != null)
                System.out.println("Retrieved event: " + event.toString());
            else
                System.out.println("[ERROR] Failed to retrieve event with name \"" + eventName + "\"");
        }
        else {
            throw new IllegalArgumentException("Unknown operation: " + operation);
        }

        System.out.println("Exiting now.");

        session.close();

        System.out.println("Closed connection to NDB cluster.");
    }
}

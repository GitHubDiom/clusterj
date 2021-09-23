import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.TableEvent;
import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.store.RecordAttr;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;
import org.apache.commons.cli.*;

import java.util.Properties;

public class ClusterJEventTest {

    private static final String DEFAULT_CONNECT_STRING = "10.241.64.15:1186";
    private static final String DEFAULT_DATABASE = "ndbexamples";
    private static final String DEFAULT_TABLE_NAME = "t0";
    private static final String DEFAULT_EVENT_NAME = "MY_EVENT_t0";

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

        options.addOption(connectStringOption);
        options.addOption(databaseOption);
        options.addOption(tableNameOption);
        options.addOption(eventNameOption);
        options.addOption(timeoutOption);

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
        int timeout = DEFAULT_TIMEOUT;

        if (cmd.hasOption("connect_string"))
            connectString = cmd.getOptionValue("connect_string");

        if (cmd.hasOption("database"))
            database = cmd.getOptionValue("database");

        if (cmd.hasOption("event_name"))
            tableName = cmd.getOptionValue("event_name");

        if (cmd.hasOption("table_name"))
            eventName = cmd.getOptionValue("table_name");

        if (cmd.hasOption("timeout"))
            timeout = Integer.parseInt(cmd.getOptionValue("timeout"));

        Properties props = new Properties();
        props.put("com.mysql.clusterj.connectstring", connectString);
        props.put("com.mysql.clusterj.database", database);

        System.out.println("Using connect string \"" + connectString + "\" to connect to NDB cluster.");
        System.out.println("Target database: " + database);

        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        String[] eventColumnNames = new String[] {
                "c0",
                "c1",
                "c2",
                "c3",
                "c4"
        };

        session.createAndRegisterEvent(
                "MY_EVENT_t0",
                tableName,
                eventColumnNames,
                new TableEvent[] { TableEvent.ALL });

        EventOperation eventOperation = session.createEventOperation(eventName);

        RecordAttr[] postAttrs = new RecordAttr[eventColumnNames.length];
        RecordAttr[] preAttrs = new RecordAttr[eventColumnNames.length];

        for (int i = 0; i < eventColumnNames.length; i++) {
            String eventColumnName = eventColumnNames[i];
            RecordAttr postAttr = eventOperation.getValue(eventColumnName);
            RecordAttr preAttr = eventOperation.getPreValue(eventColumnName);

            System.out.println("PostAttr for " + eventColumnName + ": " + postAttr.toString());
            System.out.println("PreAttr for " + eventColumnName + ": " + preAttr.toString());

            postAttrs[i] = postAttr;
            preAttrs[i] = preAttr;
        }

        int eventCounter = 0;
        while (eventCounter < timeout) {
            boolean foundEvents = session.pollEvents(1000, null);

            System.out.println("Events detected: " + foundEvents);

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

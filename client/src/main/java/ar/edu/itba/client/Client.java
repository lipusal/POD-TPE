package ar.edu.itba.client;

import ar.edu.itba.args.Util;
import ar.edu.itba.client.strategy.*;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.Timer;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] unusedArgs) throws IOException, ExecutionException, InterruptedException {
        // Parse and validate arguments
        ClientArguments args = new ClientArguments();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(Util.propertiesToArgs(System.getProperties(), ClientArguments.KNOWN_PROPERTIES).toArray(new String[0]));
        args.postValidate();

        // Configure Hazelcast client
        final ClientConfig ccfg = new ClientConfig()
                .setGroupConfig(new GroupConfig()
                        .setName(ClientArguments.GROUP_NAME)
                        .setPassword(ClientArguments.GROUP_NAME)
                )
                .setNetworkConfig(new ClientNetworkConfig()
                        .addAddress(args.getNodeIps().toArray(new String[0]))
                );

        // Start client with specified configuration
        logger.info("Client starting ...");
        final HazelcastInstance hz = com.hazelcast.client.HazelcastClient.newHazelcastClient(ccfg);

        // Track run time
        Timer timer = new Timer(args.getTimeFile());

        QueryRunner runner = getQueryRunner(hz, args);
        // Read and upload data to cluster
        logger.info("Data read start");
        timer.dataReadStart();
        runner.readData();
        runner.uploadData();
        timer.dataReadEnd();
        logger.info("Data read end");
        // Run query and save results to file
        logger.info("Query #{} start", args.getQueryNumber());
        timer.queryStart();
        runner.runQuery();
        runner.writeResult();
        timer.queryEnd();
        logger.info("Query #{} end", args.getQueryNumber());
        logger.info("Results:\n{}", runner.getResultString());

        // Done, shut down
        logger.info("Client shutting down...");
        hz.shutdown();
    }

    /**
     * Get the appropriate query runner for the specified arguments.
     *
     * @param client    The Hazelcast client on which the query will run.
     * @param arguments The query arguments.
     * @return          The appropriate query runner.
     */
    private static QueryRunner getQueryRunner(HazelcastInstance client, ClientArguments arguments) {
        switch(arguments.getQueryNumber()) {
            case 1:
                return new Q1Runner(client, arguments);
            case 2:
                return new Q2Runner(client, arguments);
            case 3:
                return new Q3Runner(client, arguments);
            case 4:
                return new Q4Runner(client, arguments);
            case 5:
                return new Q5Runner(client, arguments);
            case 6:
                return new Q6Runner(client, arguments);
            case 7:
                throw new UnsupportedOperationException("Q7 runner not implemented yet");
                // TODO: Use Q7Runner
                //return new Q7Runner(client, arguments);
            default:
                throw new IllegalArgumentException("Invalid query number " + arguments.getQueryNumber() + ". Valid numbers are 1-7.");
        }
    }
}

package ar.edu.itba.client;

import ar.edu.itba.Arguments;
import ar.edu.itba.client.util.CsvParser;
import com.beust.jcommander.JCommander;
import ar.edu.itba.client.util.Timer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws IOException {
        List<String> allArguments = Arguments.fromProperties(System.getProperties());
        allArguments.addAll(Arrays.asList(args));

        Arguments parsedArgs = new Arguments();
        JCommander.newBuilder()
                .addObject(parsedArgs)
                .build()
                .parse(allArguments.toArray(new String[0]));
        parsedArgs.postValidate();
        logger.info("Client starting ...");
        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = com.hazelcast.client.HazelcastClient.newHazelcastClient(ccfg);

        Timer timer = new Timer(new File( "time.txt"));

        // Curtanse, tiene mi directorio
        CsvParser parser = new CsvParser(new File("/Users/juanlipuma/POD-TPE/census100.csv").toPath());
        timer.dataReadStart();
        parser.parse().forEach(System.out::println);
        timer.dataReadEnd();

        // TODO upload data to cluster

        timer.queryStart();
        // TODO perform query
        timer.queryEnd();
    }
}

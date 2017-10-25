package ar.edu.itba.client;

import ar.edu.itba.Arguments;
import ar.edu.itba.client.util.CsvParser;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        Arguments parsedArgs = new Arguments();
        JCommander.newBuilder()
                .addObject(parsedArgs)
                .build()
                .parse(args);

        logger.info("Client starting ...");
        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = com.hazelcast.client.HazelcastClient.newHazelcastClient(ccfg);

        // Curtanse tiene mi directorio
        CsvParser parser = new CsvParser(new File("/Users/juanlipuma/POD-TPE/census100.csv").toPath());
        parser.parse().forEach(System.out::println);
    }
}

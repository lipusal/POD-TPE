package ar.edu.itba.client;

import ar.edu.itba.Arguments;
import ar.edu.itba.CensusEntry;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.client.util.Timer;
import ar.edu.itba.example.IdentityMapper;
import ar.edu.itba.example.IdentityReducerFactory;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // 1) Add program arguments
        List<String> allArguments = Arguments.fromProperties(System.getProperties());
        allArguments.addAll(Arrays.asList(args));
        // 2) Add system properties (-Dx=y)
        Arguments parsedArgs = new Arguments();
        // 3) Parse and validate
        JCommander.newBuilder()
                .addObject(parsedArgs)
                .build()
                .parse(allArguments.toArray(new String[0]));
        parsedArgs.postValidate();

        logger.info("Client starting ...");
        final ClientConfig ccfg = new ClientConfig();
        // TODO use configuration from arguments
        final HazelcastInstance hz = com.hazelcast.client.HazelcastClient.newHazelcastClient(ccfg);

        Timer timer = new Timer(new File("time.txt"));

        // Read data
        CsvParser parser = new CsvParser(new File("census100.csv").toPath());
        timer.dataReadStart();
        List<CensusEntry> data = parser.parse();
        timer.dataReadEnd();

        // Upload data to cluster
        JobTracker tracker = hz.getJobTracker("query" + parsedArgs.getQueryNumber());
        final IList<CensusEntry> iData = hz.getList("data");
        iData.clear();
        iData.addAll(data);
        // TODO: Consider uploading as map, ie. adding a unique key to each census entry
        final KeyValueSource<String, CensusEntry> source = KeyValueSource.fromList(iData);

        // Set up query
        Job<String, CensusEntry> job = tracker.newJob(source);
        ReducingSubmittableJob<String, String, List<CensusEntry>> future = job
                .mapper(new IdentityMapper())
                .reducer(new IdentityReducerFactory());
        timer.queryStart();

        // Submit and block until done
        Object result = future.submit().get();
        // TODO do something with result
        // FIXME stop using object for the love of God
        // TODO write result to file
        timer.queryEnd();
        System.out.println("Done");
    }
}

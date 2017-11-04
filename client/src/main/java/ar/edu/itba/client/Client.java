package ar.edu.itba.client;

import ar.edu.itba.Arguments;
import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.client.util.Timer;
import ar.edu.itba.q1.CensusQuery1Mapper;
import ar.edu.itba.q1.CensusQuery1ReducerFactory;
import ar.edu.itba.q2.CensusQuery2Collator;
import ar.edu.itba.q2.CensusQuery2CombinerFactory;
import ar.edu.itba.q2.CensusQuery2Mapper;
import ar.edu.itba.q2.CensusQuery2ReducerFactory;
import ar.edu.itba.q3.CensusQuery3Collator;
import ar.edu.itba.q3.CensusQuery3CombinerFactory;
import ar.edu.itba.q3.CensusQuery3Mapper;
import ar.edu.itba.q3.CensusQuery3ReducerFactory;
import ar.edu.itba.q4.CensusToRegionHomeIdMapper;
import ar.edu.itba.q4.HomeCountCollator;
import ar.edu.itba.q4.RegionToHomeCountReducer;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "53384-54197-54859-55824";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // 1) Add system properties (-Dx=y)
        List<String> allArguments = Arguments.fromProperties(System.getProperties());
        // 2) Add program arguments
        allArguments.addAll(Arrays.asList(args));
        // 3) Parse and validate
        Arguments parsedArgs = new Arguments();
        JCommander.newBuilder()
                .addObject(parsedArgs)
                .build()
                .parse(allArguments.toArray(new String[0]));
        parsedArgs.postValidate();

//        System.out.println("Query number: " + parsedArgs.getQueryNumber());

        logger.info("Client starting ...");
        final ClientConfig ccfg = new ClientConfig();
        // TODO use configuration from arguments
        final HazelcastInstance hz = com.hazelcast.client.HazelcastClient.newHazelcastClient(ccfg);

        Timer timer = new Timer(new File("time.txt"));

        // Read data
        System.out.println("Loading Map");
        CsvParser parser = new CsvParser(new File("census100.csv").toPath());
        logger.info("Start reading file...");
        timer.dataReadStart();
        List<CensusEntry> data = parser.parse();
        timer.dataReadEnd();
        logger.info("End of reading");

        // Upload data to cluster
        System.out.println("Getting tracker");
        JobTracker tracker = hz.getJobTracker(PREFIX + "_query" + parsedArgs.getQueryNumber());
        final IList<CensusEntry> iData = hz.getList(PREFIX + "_data");
        iData.clear();
        iData.addAll(data);

        // TODO: Consider uploading as map, ie. adding a unique key to each census entry
        final KeyValueSource<String, CensusEntry> source = KeyValueSource.fromList(iData);

        // Set up query
        System.out.println("New job");
        Job<String, CensusEntry> job = tracker.newJob(source);

        Integer queryNUmber = parsedArgs.getQueryNumber();

        switch (queryNUmber) {
            case 1:

                //What we've done

                /*ReducingSubmittableJob<String, String, List<CensusEntry>> future = job
                        .mapper(new IdentityMapper())
                        .reducer(new IdentityReducerFactory());
                timer.queryStart();

                 //Submit and block until done
                Object result = future.submit().get();
                 TODO do something with result
                 FIXME stop using object for the love of God
                 TODO write result to file
                timer.queryEnd();
                System.out.println("Done");*/


                //Added by me
                logger.info("Running map/reduce");
                timer.queryStart();
                JobCompletableFuture<Map<Region, Integer>> future1 = job.mapper(new CensusQuery1Mapper()).reducer(new CensusQuery1ReducerFactory()).submit();

                Map<Region, Integer> ans1 = future1.get();
                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                System.out.println(ans1.toString());
                break;
            case 2:
                //TODO: add query params to the configuration
                String prov = "Buenos Aires";
                int limit = 10;
                logger.info("Running map/reduce");
                timer.queryStart();

                //QUERY2
                JobCompletableFuture<List<Map.Entry<String, Integer>>> future2 = job.mapper(new CensusQuery2Mapper(prov)).combiner(new CensusQuery2CombinerFactory()).reducer(new CensusQuery2ReducerFactory()).submit(new CensusQuery2Collator(limit));

                List<Map.Entry<String , Integer>> ans2 = future2.get();

                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                System.out.println(ans2.toString());
                break;
            case 3:
                logger.info("Running map/reduce");
                timer.queryStart();

                //QUERY3
                JobCompletableFuture<Map<Region, Double>> future3 = job.mapper(new CensusQuery3Mapper()).combiner(new CensusQuery3CombinerFactory()).reducer(new CensusQuery3ReducerFactory()).submit(new CensusQuery3Collator());

                Map<Region, Double> ans3 = future3.get();

                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                for (Map.Entry<Region, Double> entry : ans3.entrySet()) {
                    System.out.printf(entry.getKey() + ",%.2f\n",entry.getValue());
                }
                break;
            case 4:
                ReducingSubmittableJob<String, Region, Integer> future = job
                        .mapper(new CensusToRegionHomeIdMapper())
                        .reducer(new RegionToHomeCountReducer());

                //Submit and block until done
                timer.queryStart();
                Map<Region, Integer> result = future.submit(new HomeCountCollator()).get();
                //TODO write result to file
                timer.queryEnd();
                System.out.println(result);

                logger.info("End of map/reduce");
                System.out.println("Done");
                break;
            case 5:
                logger.info("Running map/reduce");
                timer.queryStart();

                //QUERY5
                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                break;
            case 6:
                logger.info("Running map/reduce");
                timer.queryStart();

                //QUERY6
                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                break;
        }


    }
}

package ar.edu.itba.client;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.ProvinceTuple;
import ar.edu.itba.args.Util;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.client.util.Timer;
import ar.edu.itba.q1.CensusQuery1Mapper;
import ar.edu.itba.q1.CensusQuery1ReducerFactory;
import ar.edu.itba.q2.CensusQuery2Collator;
import ar.edu.itba.q2.CensusQuery2CombinerFactory;
import ar.edu.itba.q2.CensusQuery2Mapper;
import ar.edu.itba.q2.CensusQuery2ReducerFactory;
import ar.edu.itba.q4.CensusToRegionHomeIdMapper;
import ar.edu.itba.q4.HomeCountCollator;
import ar.edu.itba.q4.RegionToHomeCountReducer;
import ar.edu.itba.q5.CensusQuery5Collator;
import ar.edu.itba.q5.CensusQuery5Mapper;
import ar.edu.itba.q5.CensusQuery5ReducerFactory;
import ar.edu.itba.q6.CensusQuery6Collator;
import ar.edu.itba.q6.CensusQuery6CombinerFactory;
import ar.edu.itba.q6.CensusQuery6Mapper;
import ar.edu.itba.q6.CensusQuery6ReducerFactory;
import ar.edu.itba.q7.first.CensusQuery7FirstMapper;
import ar.edu.itba.q7.first.CensusQuery7FirstReducerFactory;
import ar.edu.itba.q7.second.CensusQuery7SecondCollator;
import ar.edu.itba.q7.second.CensusQuery7SecondMapper;
import ar.edu.itba.q7.second.CensusQuery7SecondReducerFactory;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

        Timer timer = new Timer(new File("time.txt"));

        // Read data
        System.out.println("Loading Map");
        CsvParser parser = new CsvParser(args.getInFile().toPath());
        logger.info("Start reading file...");
        timer.dataReadStart();
        List<CensusEntry> data = parser.parse();
        timer.dataReadEnd();
        logger.info("End of reading");

        // Upload data to cluster
        System.out.println("Getting tracker");
        JobTracker tracker = hz.getJobTracker(ClientArguments.GROUP_NAME + "_query" + args.getQueryNumber());
        final IList<CensusEntry> iData = hz.getList(ClientArguments.GROUP_NAME + "_data");
        iData.clear();
        iData.addAll(data);

        // TODO: Consider uploading as map, ie. adding a unique key to each census entry
        final KeyValueSource<String, CensusEntry> source = KeyValueSource.fromList(iData);

        // Set up query
        System.out.println("New job");
        Job<String, CensusEntry> job = tracker.newJob(source);

        Integer queryNUmber = args.getQueryNumber();

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

                List<Map.Entry<String, Integer>> ans2 = future2.get();

                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                System.out.println(ans2.toString());
                break;
            case 3:
                logger.info("Running map/reduce");
                timer.queryStart();

                //QUERY3
                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
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
                ReducingSubmittableJob<String, Region, Double> future3 = job
                        .mapper(new CensusQuery5Mapper())
                        .reducer(new CensusQuery5ReducerFactory());

                //Submit and block until done
                timer.queryStart();
                Map<Region, Double> result3 = future3.submit(new CensusQuery5Collator()).get();

                //QUERY5
                timer.queryEnd();
                for(Map.Entry<Region, Double> entry : result3.entrySet()){
                    System.out.printf(Locale.US, "%s,%.2f\n",entry.getKey().toString(),entry.getValue());
                }

                logger.info("End of map/reduce");
                System.out.println("Done");
                break;
            case 6:
                int minLimit = 5;
                logger.info("Running map/reduce");
                timer.queryStart();

                //QUERY6
                JobCompletableFuture<Map<String, Integer>> future6 = job.mapper(new CensusQuery6Mapper()).combiner(new CensusQuery6CombinerFactory()).reducer(new CensusQuery6ReducerFactory()).submit(new CensusQuery6Collator(minLimit));

                Map<String, Integer> ans6 = future6.get();

                timer.queryEnd();
                logger.info("End of map/reduce");
                System.out.println("Done");
                System.out.println(ans6.toString());
                break;
            case 7:
                logger.info("Running map/reduce");
                ReducingSubmittableJob<String, String, Set<String>> future7 = job
                        .mapper(new CensusQuery7FirstMapper())
                        .reducer(new CensusQuery7FirstReducerFactory());

                //Submit and block until done
                timer.queryStart();
                Map<String, Set<String>> result7 = future7.submit().get();

                System.out.println("---RESULTS7---");
                for(Map.Entry<String, Set<String>> entry : result7.entrySet()){
                    System.out.println("** " + entry.getKey() + " ***");
                    for(String s : entry.getValue()){
                        System.out.print(s + " - ");
                    }
                    System.out.println("");
                }

                //Second Map
                IMap<String, Set<String>> iData7 = hz.getMap(ClientArguments.GROUP_NAME + "_map");
                iData7.clear();
                iData7.putAll(result7);
                KeyValueSource<String, Set<String>> source7 = KeyValueSource.fromMap(iData7);

                Job<String, Set<String>> job7 = tracker.newJob(source7);

                ReducingSubmittableJob<String, ProvinceTuple, Integer> future72 = job7
                        .mapper(new CensusQuery7SecondMapper())
                        .reducer(new CensusQuery7SecondReducerFactory());

                Map<String, Integer> result72 = future72.submit(new CensusQuery7SecondCollator(0)).get();

                System.out.println("----------Result72-------------");
                for(Map.Entry<String, Integer> entry : result72.entrySet()){
                    System.out.println(entry.getKey().split("=")[0] + "," + entry.getValue());
                }
                System.out.println("");

                timer.queryEnd();


                logger.info("End of map/reduce");
                System.out.println("Done");
                break;
        }
    }
}

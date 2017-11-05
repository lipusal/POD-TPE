package ar.edu.itba.client;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.ProvinceTuple;
import ar.edu.itba.args.Util;
import ar.edu.itba.client.strategy.Q2Runner;
import ar.edu.itba.client.strategy.Q3Runner;
import ar.edu.itba.client.strategy.Q4Runner;
import ar.edu.itba.client.strategy.Q5Runner;
import ar.edu.itba.client.strategy.Q6Runner;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.client.util.Timer;
import ar.edu.itba.q1.CensusQuery1Mapper;
import ar.edu.itba.q1.CensusQuery1ReducerFactory;
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
                // TODO delete all this code
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
                // TODO delete up to here and replace with the following
//                Q1Runner runner = new Q1Runner(hz, args);
//                runner.readData();
//                runner.uploadData();
//                timer.queryStart();
//                runner.runQuery();
//                runner.writeResult();
//                timer.queryEnd();
//                System.out.println("Done");
//                System.out.println(runner.getResult());
                break;
            case 2:
                Q2Runner runner = new Q2Runner(hz, args);
                runner.readData();
                runner.uploadData();
                timer.queryStart();
                runner.runQuery();
                runner.writeResult();
                timer.queryEnd();
                System.out.println("Done");
                System.out.println(runner.getResultString());
                break;
            case 3:
                Q3Runner runner = new Q3Runner(hz, args);
                runner.readData();
                runner.uploadData();
                timer.queryStart();
                runner.runQuery();
                runner.writeResult();
                timer.queryEnd();
                System.out.println("Done");
                System.out.println(runner.getResultString());
                break;
            case 4:
                Q4Runner runner = new Q4Runner(hz, args);
                runner.readData();
                runner.uploadData();
                timer.queryStart();
                runner.runQuery();
                runner.writeResult();
                timer.queryEnd();
                System.out.println("Done");
                System.out.println(runner.getResultString());
                break;
            case 5:
                Q5Runner runner = new Q5Runner(hz, args);
                runner.readData();
                runner.uploadData();
                timer.queryStart();
                runner.runQuery();
                runner.writeResult();
                timer.queryEnd();
                System.out.println("Done");
                System.out.println(runner.getResultString());
                break;
            case 6:
                Q6Runner runner = new Q6Runner(hz, args);
                runner.readData();
                runner.uploadData();
                timer.queryStart();
                runner.runQuery();
                runner.writeResult();
                timer.queryEnd();
                System.out.println("Done");
                System.out.println(runner.getResultString());
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

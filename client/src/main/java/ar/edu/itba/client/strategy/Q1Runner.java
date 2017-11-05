package ar.edu.itba.client.strategy;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q1.CensusQuery1Mapper;
import ar.edu.itba.q1.CensusQuery1ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Q1Runner extends BaseQueryRunner {
    private Map<Region, Integer> result;

    public Q1Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<String, CensusEntry> keyValueSource = KeyValueSource.fromList(iData);
        Job<String, CensusEntry> job = getJobTracker().newJob(keyValueSource);
        JobCompletableFuture<Map<Region, Integer>> completableFuture = job.mapper(new CensusQuery1Mapper()).reducer(new CensusQuery1ReducerFactory()).submit();

        result = completableFuture.get();
        System.out.println(result.toString());
    }

    @Override
    public void writeResult() throws IOException {
        FileWriter fw = new FileWriter(arguments.getOutFile());
        for (Map.Entry<Region, Integer> entry : result.entrySet()) {
            fw.write(entry.getKey() + "," + entry.getValue() + "\n");

        }
        fw.close();
    }

    public Map<Region, Integer> getResult() {
        return result;
    }
}

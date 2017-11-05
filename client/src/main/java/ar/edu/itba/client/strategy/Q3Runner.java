package ar.edu.itba.client.strategy;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q3.CensusQuery3Collator;
import ar.edu.itba.q3.CensusQuery3CombinerFactory;
import ar.edu.itba.q3.CensusQuery3Mapper;
import ar.edu.itba.q3.CensusQuery3ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class Q3Runner extends BaseQueryRunner {

    private IList<CensusEntry> iData;
    private Map<Region, Double> result;

    public Q3Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<String, CensusEntry> keyValueSource = KeyValueSource.fromList(iData);
        Job<String, CensusEntry> job = getJobTracker().newJob(keyValueSource);
        JobCompletableFuture<Map<Region, Double>> future3 = job.mapper(new CensusQuery3Mapper()).combiner(new CensusQuery3CombinerFactory()).reducer(new CensusQuery3ReducerFactory()).submit(new CensusQuery3Collator());
        result = future3.get();
    }

    @Override
    public void writeResult() throws IOException {
        FileWriter fw = new FileWriter(arguments.getOutFile());
        fw.write(getResultString());
        fw.close();
    }

    @Override
    public void printResult() {
        System.out.println(getResultString());
    }

    private String getResultString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<Region, Double> entry : result.entrySet()) {
            stringBuilder.append(entry.getKey()).append(String.format(",%.2f\n", entry.getValue()));
        }
        return stringBuilder.toString();
    }
}

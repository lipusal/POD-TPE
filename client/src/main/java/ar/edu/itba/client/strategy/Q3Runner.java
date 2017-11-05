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
    private Map<Region, Double> result;

    public Q3Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<String, CensusEntry> keyValueSource = KeyValueSource.fromList(iData);
        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery3Mapper())
                .combiner(new CensusQuery3CombinerFactory())
                .reducer(new CensusQuery3ReducerFactory())
                .submit(new CensusQuery3Collator()).get();
    }

    @Override
    public String getResultString() {
        StringBuilder stringBuilder = new StringBuilder();
        result.forEach((key, value) -> stringBuilder.append(key).append(String.format(",%.2f\n", value)));
        return stringBuilder.toString();
    }
}

package ar.edu.itba.client.strategy;


import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.q4.CensusToRegionHomeIdMapper;
import ar.edu.itba.q4.HomeCountCollator;
import ar.edu.itba.q4.RegionToHomeCountReducer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Q4Runner extends BaseQueryRunner {
    private Map<Region, Integer> result;

    public Q4Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<String, CensusEntry> keyValueSource = KeyValueSource.fromList(iData);
        Job<String, CensusEntry> job = getJobTracker().newJob(keyValueSource);
        JobCompletableFuture<Map<Region, Integer>> completableFuture = job
                .mapper(new CensusToRegionHomeIdMapper())
                .reducer(new RegionToHomeCountReducer())
                .submit(new HomeCountCollator());
        result = completableFuture.get();
    }

    @Override
    public String getResultString() {
        StringBuilder stringBuilder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((key, value) ->
                stringBuilder.append(key).append(",").append(value).append("\n"));
        return stringBuilder.toString();
    }
}

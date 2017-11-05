package ar.edu.itba.client.strategy;


import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q6.CensusQuery6Collator;
import ar.edu.itba.q6.CensusQuery6CombinerFactory;
import ar.edu.itba.q6.CensusQuery6Mapper;
import ar.edu.itba.q6.CensusQuery6ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Q6Runner extends BaseQueryRunner {
    private Map<String, Integer> result;
    private IMap<Long, String> iData;
    private Map<Long, String> dataMap;
    private long id = 1;

    public Q6Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void readData() {
        CsvParser parser = new CsvParser(arguments.getInFile().toPath());
        dataMap = new HashMap<>();
        parser.parse(splitLine ->
                dataMap.put(id++, CsvParser.getDepartment(splitLine))
        );
    }

    @Override
    public void uploadData() {
        iData = client.getMap(getCollectionName());
        iData.clear();
        iData.putAll(dataMap);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<Long, String> keyValueSource = KeyValueSource.fromMap(iData);
        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery6Mapper())
                .combiner(new CensusQuery6CombinerFactory())
                .reducer(new CensusQuery6ReducerFactory())
                .submit(new CensusQuery6Collator(arguments.getN())).get();
    }

    @Override
    public String getResultString() {
        StringBuilder builder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((key, value) ->
                builder.append(key).append(",").append(value).append("\n"));
        return builder.toString();
    }
}

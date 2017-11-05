package ar.edu.itba.client.strategy;

import ar.edu.itba.Region;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q1.CensusQuery1CombinerFactory;
import ar.edu.itba.q1.CensusQuery1Mapper;
import ar.edu.itba.q1.CensusQuery1ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Q1Runner extends BaseQueryRunner {
    private long id = 1;
    private Map<Long, Region> dataMap;
    private IMap<Long, Region> iData;
    private Map<Region, Integer> result;

    public Q1Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void readData() {
        CsvParser parser = new CsvParser(arguments.getInFile().toPath());
        dataMap = new HashMap<>();
        parser.parse(splitLine ->
            dataMap.put(id++, Region.fromString(CsvParser.getProvince(splitLine)))
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
        KeyValueSource<Long, Region> keyValueSource = KeyValueSource.fromMap(iData);
        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery1Mapper())
                .combiner(new CensusQuery1CombinerFactory())
                .reducer(new CensusQuery1ReducerFactory())
                .submit().get();
    }

    @Override
    public String getResultString() {
        StringBuilder builder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((key, value) ->
                builder.append(key)
                        .append(",")
                        .append(value)
                        .append("\n"));
        return builder.toString();
    }
}

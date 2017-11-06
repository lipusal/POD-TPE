package ar.edu.itba.client.strategy;

import ar.edu.itba.Region;
import ar.edu.itba.Status;
import ar.edu.itba.Tuple;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q3.CensusQuery3Collator;
import ar.edu.itba.q3.CensusQuery3CombinerFactory;
import ar.edu.itba.q3.CensusQuery3Mapper;
import ar.edu.itba.q3.CensusQuery3ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class Q3Runner extends BaseQueryRunner {
    private long id = 1;
    private Map<Long, Tuple<Region, Status>> dataMap;
    private IMap<Long, Tuple<Region, Status>> iData;
    private Map<Region, Double> result;

    public Q3Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void readData() {
        CsvParser parser = new CsvParser(arguments.getInFile().toPath());
        dataMap = new HashMap<>();
        parser.parse(splitLine ->
            dataMap.put(id++, new Tuple<>(Region.fromString(CsvParser.getProvince(splitLine)), CsvParser.getStatus(splitLine)))
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
        KeyValueSource<Long, Tuple<Region, Status>> keyValueSource = KeyValueSource.fromMap(iData);
        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery3Mapper())
                .combiner(new CensusQuery3CombinerFactory())
                .reducer(new CensusQuery3ReducerFactory())
                .submit(new CensusQuery3Collator()).get();
    }

    @Override
    public String getResultString() {
        StringBuilder stringBuilder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((key, value) ->
                stringBuilder
                        .append(key)
                        .append(",")
                        .append(formatDecimal(value))
                        .append("\n"));
        return stringBuilder.toString();
    }
}

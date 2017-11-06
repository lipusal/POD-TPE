package ar.edu.itba.client.strategy;

import ar.edu.itba.Region;
import ar.edu.itba.Tuple;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q5.CensusQuery5Collator;
import ar.edu.itba.q5.CensusQuery5Mapper;
import ar.edu.itba.q5.CensusQuery5ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Q5Runner extends BaseQueryRunner {
    private Map<Region, Double> result;
    private IMap<Long, Tuple<Region, Long>> iData;
    private Map<Long, Tuple<Region, Long>> dataMap;
    private long id = 1;

    public Q5Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void readData() {
        CsvParser parser = new CsvParser(arguments.getInFile().toPath());
        dataMap = new HashMap<>();
        parser.parse(splitLine ->
                dataMap.put(id++, new Tuple<>(Region.fromString(CsvParser.getProvince(splitLine)), CsvParser.getHomeId(splitLine)))
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
        KeyValueSource<Long, Tuple<Region, Long>> keyValueSource = KeyValueSource.fromMap(iData);

        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery5Mapper())
                .reducer(new CensusQuery5ReducerFactory())
                .submit(new CensusQuery5Collator()).get();
    }

    @Override
    public String getResultString() {
        StringBuilder builder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((key, value) ->
                builder
                        .append(key.toString())
                        .append(",")
                        .append(formatDecimal(value))
                        .append("\n")
        );
        return builder.toString();
    }
}

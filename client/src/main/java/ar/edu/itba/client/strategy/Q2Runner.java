package ar.edu.itba.client.strategy;

import ar.edu.itba.Tuple;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import ar.edu.itba.q2.CensusQuery2Collator;
import ar.edu.itba.q2.CensusQuery2CombinerFactory;
import ar.edu.itba.q2.CensusQuery2Mapper;
import ar.edu.itba.q2.CensusQuery2ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Q2Runner extends BaseQueryRunner {
    private long id = 1;
    private Map<Long, Tuple<String, String>> dataMap;
    private IMap<Long, Tuple<String, String>> iData;
    private List<Map.Entry<String, Integer>> result;

    public Q2Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void readData() {
        CsvParser parser = new CsvParser(arguments.getInFile().toPath());
        dataMap = new HashMap<>();
        parser.parse(splitLine ->
            dataMap.put(id++, new Tuple<>(CsvParser.getDepartment(splitLine), CsvParser.getProvince(splitLine)))
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
        KeyValueSource<Long, Tuple<String, String>> keyValueSource = KeyValueSource.fromMap(iData);

        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery2Mapper(arguments.getProv()))
                .combiner(new CensusQuery2CombinerFactory())
                .reducer(new CensusQuery2ReducerFactory())
                .submit(new CensusQuery2Collator(arguments.getN()))
                .get();
    }

    @Override
    public String getResultString() {
        StringBuilder builder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyList()).forEach(stringIntegerEntry ->
                builder.append(stringIntegerEntry.getKey())
                        .append(",")
                        .append(stringIntegerEntry.getValue())
                        .append("\n")
        );
        return builder.toString();
    }
}

package ar.edu.itba.client.strategy;


import ar.edu.itba.CensusEntry;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.q7.first.CensusQuery7FirstMapper;
import ar.edu.itba.q7.first.CensusQuery7FirstReducerFactory;
import ar.edu.itba.q7.second.CensusQuery7SecondCollator;
import ar.edu.itba.q7.second.CensusQuery7SecondMapper;
import ar.edu.itba.q7.second.CensusQuery7SecondReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MonolithicQ7Runner extends BaseQueryRunner {
    private final String STEP_1_COLLECTION;
    private final String STEP_2_COLLECTION;
    private Map<String, Integer> result;

    public MonolithicQ7Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
        STEP_1_COLLECTION = getCollectionName() + "_1";
        STEP_2_COLLECTION = getCollectionName() + "_2";
    }

    @Override
    public void uploadData() {
        iData = client.getMap(STEP_1_COLLECTION);
        iData.clear();
        iData.putAll(dataMap);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        // First step
        KeyValueSource<String, CensusEntry> source1 = KeyValueSource.fromMap(iData);
        Map<String, Set<String>> tempResult = getJobTracker().newJob(source1)
                .mapper(new CensusQuery7FirstMapper())
                .reducer(new CensusQuery7FirstReducerFactory())
                .submit().get();
        // Second step, use new distributed collection
        IMap<String, Set<String>> iData2 = client.getMap(STEP_2_COLLECTION);
        iData2.clear();
        iData2.putAll(tempResult);
        KeyValueSource<String, Set<String>> source2 = KeyValueSource.fromMap(iData2);
        result = getJobTracker().newJob(source2)
                .mapper(new CensusQuery7SecondMapper())
                .reducer(new CensusQuery7SecondReducerFactory())
                .submit(new CensusQuery7SecondCollator(arguments.getN())).get();
    }

    @Override
    public String getResultString() {
        StringBuilder builder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((s, integer) ->
                builder.append(s.split("=")[0])
                    .append(",")
                    .append(integer)
                    .append("\n"));
        return builder.toString();
    }
}

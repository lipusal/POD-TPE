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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Q7Runner extends BaseQueryRunner {
    private final String STEP_1_COLLECTION, STEP_2_COLLECTION;
    private final Step1Runner step1Runner;
    private final Step2Runner step2Runner;

    public Q7Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
        STEP_1_COLLECTION = getCollectionName() + "_1";
        STEP_2_COLLECTION = getCollectionName() + "_2";
        step1Runner = new Step1Runner(client, arguments);
        step2Runner = new Step2Runner(client, arguments);
    }

    @Override
    public void readData() {
        step1Runner.readData();
    }

    @Override
    public void uploadData() {
        step1Runner.uploadData();
    }

    @Override
    public void writeResult() throws IOException {
        step2Runner.writeResult();
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        // First step
        step1Runner.runQuery();
        // Second step, use result of first step as data (upload as new distributed collection)
        IMap<String, Set<String>> step2Data = client.getMap(STEP_2_COLLECTION);
        step2Data.clear();
        step2Data.putAll(step1Runner.result);
        // Run second step
        step2Runner.readData();
        step2Runner.runQuery();
    }

    @Override
    public String getResultString() {
        return step2Runner.getResultString();
    }

    private class Step1Runner extends BaseQueryRunner {
        private Map<String, Set<String>> result;

        Step1Runner(HazelcastInstance client, ClientArguments arguments) {
            super(client, arguments);
        }

        @Override
        public void uploadData() {
            iData = client.getList(Q7Runner.this.STEP_1_COLLECTION);
            iData.clear();
            iData.addAll(data);
        }

        @Override
        public void runQuery() throws ExecutionException, InterruptedException {
            KeyValueSource<String, CensusEntry> source = KeyValueSource.fromList(iData);
            result = getJobTracker().newJob(source)
                    .mapper(new CensusQuery7FirstMapper())
                    .reducer(new CensusQuery7FirstReducerFactory())
                    .submit().get();
        }

        @Override
        public String getResultString() {
            StringBuilder builder = new StringBuilder();
            result.forEach((s, strings) -> {
                builder.append(s).append(": ");
                strings.forEach(s1 -> builder.append(s1).append(" - "));
                builder.append("\n");
            });
            return builder.toString();
        }
    }

    private class Step2Runner extends BaseQueryRunner {
        private IMap<String, Set<String>> iMap;
        private Map<String, Integer> result;

        Step2Runner(HazelcastInstance client, ClientArguments arguments) {
            super(client, arguments);
        }

        @Override
        public void readData() {
            iMap = client.getMap(Q7Runner.this.STEP_2_COLLECTION);
        }

        @Override
        public void uploadData() {
            // Do nothing, data is already uploaded in step 1
        }

        @Override
        public void runQuery() throws ExecutionException, InterruptedException {
            KeyValueSource<String, Set<String>> source = KeyValueSource.fromMap(iMap);
            result = getJobTracker().newJob(source)
                    .mapper(new CensusQuery7SecondMapper())
                    .reducer(new CensusQuery7SecondReducerFactory())
                    .submit(new CensusQuery7SecondCollator(arguments.getN())).get();
        }

        @Override
        public String getResultString() {
            StringBuilder builder = new StringBuilder();
            result.forEach((s, integer) ->
                    builder.append(s.split("=")[0])
                            .append(",")
                            .append(integer)
                            .append("\n"));
            return builder.toString();
        }
    }
}

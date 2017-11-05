package ar.edu.itba.client.strategy;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.q2.CensusQuery2Collator;
import ar.edu.itba.q2.CensusQuery2CombinerFactory;
import ar.edu.itba.q2.CensusQuery2Mapper;
import ar.edu.itba.q2.CensusQuery2ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Q2Runner extends BaseQueryRunner {
    private List<Map.Entry<String, Integer>> result;

    public Q2Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<String, CensusEntry> keyValueSource = KeyValueSource.fromList(iData);

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

package ar.edu.itba.client.strategy;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Region;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.q5.CensusQuery5Collator;
import ar.edu.itba.q5.CensusQuery5Mapper;
import ar.edu.itba.q5.CensusQuery5ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Q5Runner extends BaseQueryRunner {
    private Map<Region, Double> result;

    public Q5Runner(HazelcastInstance client, ClientArguments arguments) {
        super(client, arguments);
    }

    @Override
    public void runQuery() throws ExecutionException, InterruptedException {
        KeyValueSource<String, CensusEntry> keyValueSource = KeyValueSource.fromList(iData);

        result = getJobTracker().newJob(keyValueSource)
                .mapper(new CensusQuery5Mapper())
                .reducer(new CensusQuery5ReducerFactory())
                .submit(new CensusQuery5Collator()).get();
    }

    @Override
    public String getResultString() {
        StringBuilder builder = new StringBuilder();
        Optional.ofNullable(result).orElse(Collections.emptyMap()).forEach((key, value) ->
                builder.append(String.format(Locale.US, "%s,%.2f\n", key.toString(), value))
        );
        return builder.toString();
    }
}

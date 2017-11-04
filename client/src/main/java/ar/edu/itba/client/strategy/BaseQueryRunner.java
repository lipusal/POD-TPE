package ar.edu.itba.client.strategy;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.client.util.ClientArguments;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;

import java.util.List;

public abstract class BaseQueryRunner implements QueryRunner {
    protected final HazelcastInstance client;
    protected final ClientArguments arguments;
    protected List<CensusEntry> data;

    public BaseQueryRunner(HazelcastInstance client, ClientArguments arguments) {
        this.client = client;
        this.arguments = arguments;
    }

    protected JobTracker getJobTracker() {
        return client.getJobTracker(ClientArguments.GROUP_NAME + "_query" + arguments.getQueryNumber());
    }

    protected String getCollectionName() {
        return ClientArguments.GROUP_NAME + "_data_" + arguments.getQueryNumber();
    }
}

package ar.edu.itba.client.strategy;

import ar.edu.itba.client.util.ClientArguments;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;

import java.io.FileWriter;
import java.io.IOException;

public abstract class BaseQueryRunner implements QueryRunner {
    protected final HazelcastInstance client;
    protected final ClientArguments arguments;

    public BaseQueryRunner(HazelcastInstance client, ClientArguments arguments) {
        this.client = client;
        this.arguments = arguments;
    }

    @Override
    public void writeResult() throws IOException {
        FileWriter fw = new FileWriter(arguments.getOutFile());
        fw.write(getResultString());
        fw.close();
    }

    protected JobTracker getJobTracker() {
        return client.getJobTracker(ClientArguments.GROUP_NAME + "_query" + arguments.getQueryNumber());
    }

    protected String getCollectionName() {
        return ClientArguments.GROUP_NAME + "_data";
    }
}

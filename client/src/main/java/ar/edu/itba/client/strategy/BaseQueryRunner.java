package ar.edu.itba.client.strategy;

import ar.edu.itba.client.util.ClientArguments;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Locale;

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

    /**
     * Truncate - don't round - numbers to a given number of decimal places
     * @param value     The value to truncate
     * @param precision The precision to truncate to
     * @return          The truncated value
     */
    protected String truncateDecimal(double value, int precision) {
        return String.format(Locale.US, "%."+(precision+1)+"f", value).replaceAll("\\d$", "");
    }
}

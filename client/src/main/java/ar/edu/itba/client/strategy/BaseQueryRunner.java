package ar.edu.itba.client.strategy;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.client.util.ClientArguments;
import ar.edu.itba.client.util.CsvParser;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobTracker;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseQueryRunner implements QueryRunner {
    protected final HazelcastInstance client;
    protected final ClientArguments arguments;
    protected List<CensusEntry> data;
    protected Map<Long, CensusEntry> dataMap;
    protected IMap<Long, CensusEntry> iData;
    private long key = 1;


    public BaseQueryRunner(HazelcastInstance client, ClientArguments arguments) {
        this.client = client;
        this.arguments = arguments;
    }

    /**
     * Read data from file specified in arguments and save it as {@code List<CensusEntry>}.
     */
    @Override
    public void readData() {
        CsvParser parser = new CsvParser(arguments.getInFile().toPath());
        data = parser.parse();
        this.dataMap = new HashMap<>(data.size());
        data.forEach(censusEntry -> dataMap.put(key++, censusEntry));
    }

    /**
     * Upload data read from {@link #readData()} as an {@link IList}.
     */
    @Override
    public void uploadData() {
        iData = client.getMap(getCollectionName());
        iData.clear();
        iData.putAll(dataMap);
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
        return ClientArguments.GROUP_NAME + "_data_" + arguments.getQueryNumber();
    }
}

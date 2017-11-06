package ar.edu.itba.client.strategy;

import ar.edu.itba.client.util.ClientArguments;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;

import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public abstract class BaseQueryRunner implements QueryRunner {
    protected final HazelcastInstance client;
    protected final ClientArguments arguments;
    private final DecimalFormat decimalFormat;

    public BaseQueryRunner(HazelcastInstance client, ClientArguments arguments) {
        this.client = client;
        this.arguments = arguments;

        decimalFormat = new DecimalFormat();
        decimalFormat.setDecimalFormatSymbols(DecimalFormatSymbols.getInstance(Locale.US));    // Separate with .
        decimalFormat.setGroupingUsed(false);                                                  // Don't group with ,
        decimalFormat.setMinimumFractionDigits(2);                                             // Use exactly 2 digits
        decimalFormat.setMaximumFractionDigits(2);
        decimalFormat.setRoundingMode(RoundingMode.FLOOR);                                     // Don't round
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
     * Truncate - don't round - numbers to 2 decimal places
     * @param value     The value to truncate
     * @return          The truncated value
     */
    protected String formatDecimal(double value) {
        return decimalFormat.format(value);
    }
}

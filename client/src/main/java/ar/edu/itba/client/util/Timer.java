package ar.edu.itba.client.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class used to keep track of when certain events happened during the course of the program, in order to export them
 * when the program ends.
 */
public class Timer {
    private enum Status {NOT_STARTED, READING_DATA, READ_DATA, EXECUTING_QUERY, DONE}

    private final FileWriter timeWriter;
    private Status status;
    private List<LocalDateTime> timestamps;
    private static final List<String> labels = Arrays.asList("Data read start", "Data read end", "Query start", "Query end");

    public Timer(File timeFile) throws IOException {
        this.timeWriter = new FileWriter(timeFile);
        this.timestamps = new ArrayList<>();
        this.status = Status.NOT_STARTED;
    }

    /**
     * Save data read start time.
     *
     * @throws IllegalStateException If called in an invalid state.
     */
    public void dataReadStart() throws IllegalStateException {
        if (status != Status.NOT_STARTED) {
            throw new IllegalStateException("dataReadStart() can only be called once, and before any other start/end methods");
        }
        saveTimestamp();
        status = Status.READING_DATA;
    }

    /**
     * Save data read end time.
     *
     * @throws IllegalStateException If called at any time other than after calling {@link #dataReadStart()}.
     */
    public void dataReadEnd() throws IllegalStateException {
        if (status != Status.READING_DATA) {
            throw new IllegalStateException("dataReadEnd() can only be called once, after dataReadStart()");
        }
        saveTimestamp();
        status = Status.READ_DATA;
    }

    /**
     * Save query start time.
     *
     * @throws IllegalStateException If called at any time other than after calling {@link #dataReadEnd()}
     */
    public void queryStart() throws IllegalStateException {
        if (status != Status.READ_DATA) {
            throw new IllegalStateException("queryStart() can only be called once, after dataReadEnd()");
        }
        saveTimestamp();
        status = Status.EXECUTING_QUERY;
    }

    /**
     * Save query end time, write all saved timestamps to file, and close it.
     *
     * @throws IllegalStateException If called at any time other than after calling {@link #queryStart()}
     * @throws IOException           On I/O error with the time file.
     */
    public void queryEnd() throws IllegalStateException, IOException {
        if (status != Status.EXECUTING_QUERY) {
            throw new IllegalStateException("queryEnd() can only be called once, after queryStart()");
        }
        saveTimestamp();
        status = Status.DONE;
        writeTimestamps();
        timeWriter.close();
    }

    /**
     * Append the current timestamp to saved timestamps.
     */
    private void saveTimestamp() {
        timestamps.add(LocalDateTime.now());
    }

    /**
     * Write all saved timestamps in the time file, one per line.
     */
    private void writeTimestamps() throws IOException {
        if(timestamps.size() != labels.size()) {
            throw new IllegalArgumentException("Not ready to write timestamps, need " + labels.size() + " but have " + timestamps.size());
        }
        for (int i = 0; i < labels.size(); i++) {
            timeWriter.write(timestamps.get(i) + " - " + labels.get(i));
        }
    }
}

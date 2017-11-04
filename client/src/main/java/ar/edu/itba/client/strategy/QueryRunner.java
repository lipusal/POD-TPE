package ar.edu.itba.client.strategy;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface QueryRunner {

    /**
     * Read all local data, e.g. from a file. Usually this data is stored in an instance variable for upload in
     * {@link #uploadData()}.
     */
    void readData();

    /**
     * Upload all data read in {@link #readData()} to the configured cluster.
     */
    void uploadData();

    /**
     * Run the query with the loaded data in the configured cluster.  Blocks until result is retrieved.
     */
    void runQuery() throws ExecutionException, InterruptedException;

    /**
     * Write the retrieved result to a file.
     * @throws IOException On I/O errors.
     */
    void writeResult() throws IOException;
}

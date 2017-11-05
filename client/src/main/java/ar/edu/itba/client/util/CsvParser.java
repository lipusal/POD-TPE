package ar.edu.itba.client.util;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Status;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class CsvParser {
    private final Path path;

    public CsvParser(Path path) {
        this.path = path;
    }

    public Map<Long, CensusEntry> parse() {
        Map<Long, CensusEntry> result = new HashMap<>();
        long key = 1;
        parse(censusEntry -> result.put(key, lineConsumer.apply(censusEntry)));
        return result;
    }

    public void parse(Consumer<String[]> consumer) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(path.toFile()));
            String line;
            while((line = br.readLine()) != null) {
                consumer.accept(line.split(","));
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Status getStatus(String[] splitLine) {
        return Status.fromInt(Integer.valueOf(splitLine[0]));
    }

    public static long getHomeId(String[] data) {
        return Long.parseLong(data[1]);
    }

    public static String getDepartment(String[] splitLine) {
        return splitLine[2];
    }

    public static String getProvince(String[] splitLine) {
        return splitLine[3];
    }

    private static final Function<String[], CensusEntry> lineConsumer = splitLine ->
        new CensusEntry(Status.fromInt(Integer.valueOf(splitLine[0])), Long.parseLong(splitLine[1]), splitLine[2], splitLine[3]);
}

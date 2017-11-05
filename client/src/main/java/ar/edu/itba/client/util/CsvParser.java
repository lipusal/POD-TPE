package ar.edu.itba.client.util;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Status;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class CsvParser {
    private final Path path;

    public CsvParser(Path path) {
        this.path = path;
    }

    public Map<Long, CensusEntry> parse() {
        Map<Long, CensusEntry> result = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(path.toFile()));
            String line;
            long key = 1;
            while((line = br.readLine()) != null) {
                result.put(key++, lineConsumer.apply(line));
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static final Function<String, CensusEntry> lineConsumer = line -> {
        String[] data = line.split(",");
        return new CensusEntry(Status.fromInt(Integer.valueOf(data[0])), Long.parseLong(data[1]), data[2], data[3]);
    };
}

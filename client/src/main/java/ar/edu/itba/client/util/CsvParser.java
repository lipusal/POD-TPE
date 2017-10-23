package ar.edu.itba.client.util;

import ar.edu.itba.CensusEntry;
import ar.edu.itba.Status;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvParser {
    private final Path path;

    public CsvParser(Path path) {
        this.path = path;
    }

    public List<CensusEntry> parse() {
        List<CensusEntry> result = null;
        try {
            Stream<String> lines = Files.lines(path);
            result = lines.map(lineConsumer).collect(Collectors.toList());
            lines.close();

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

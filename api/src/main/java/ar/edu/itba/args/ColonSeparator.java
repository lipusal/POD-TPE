package ar.edu.itba.args;

import com.beust.jcommander.IStringConverter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Splits a single comma-separated string argument to various strings.
 */
public class ColonSeparator implements IStringConverter<List<String>> {

    @Override
    public List<String> convert(String line) {
        return Arrays.stream(line.split(";")).collect(Collectors.toList());
    }
}

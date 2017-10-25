package ar.edu.itba;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.beust.jcommander.converters.IntegerConverter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * POJO for run arguments. Serializable for parameters to be sent over the net to nodes.
 */
public class Arguments implements DataSerializable {

    private static final List<String> KNOWN_PROPERTIES = Arrays.asList("addresses", "query", "inPath", "outPath", "timeOutPath", "n", "prov");

    @Parameter(names = {"-addresses", "-a"}, description = "Node IP addresses", required = true, listConverter = CommaSeparator.class)  // TODO use IP class directly?
    private String[] nodeIps;

    @Parameter(names = {"-query", "-q"}, description = "Query number to execute", required = true, converter = IntegerConverter.class) // TODO validate min/max
    private Integer queryNumber;

    @Parameter(names = {"-inPath", "-i"}, description = "File from which to read data", required = true, converter = FileConverter.class, validateWith = FileExistsValidator.class)
    private File inFile;

    @Parameter(names = {"-outPath", "-o"}, description = "File to which to output result", required = true, converter = FileConverter.class)
    private File outFile;

    @Parameter(names = {"-timeOutPath", "-t"}, description = "File to which to output execution time", required = true, converter = FileConverter.class)
    private File timeFile;

    @Parameter(names = {"-n"}, converter = IntegerConverter.class)  // TODO validate only specified for queryNumber 2, 6, 7
    private Integer n = -1;

    @Parameter(names = {"-prov"}, converter = IntegerConverter.class)  // TODO validate only specified for queryNumber 2
    private Integer prov = -1;

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        // TODO
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        // TODO
    }

    /**
     * Capture known system properties and convert them to regular program arguments, as recognized by JCommander.
     *
     * @param properties The properties to read.
     * @return The present recognized properties, in regular program argument format.
     * @see #KNOWN_PROPERTIES
     */
    public static List<String> fromProperties(Properties properties) {
        return KNOWN_PROPERTIES.stream()
                .map(propertyName -> {
                    Optional<String> value = Optional.ofNullable(properties.getProperty(propertyName));
                    // If present, transform "-Dkey=value" to "-key", "value". Else null
                    return value.map(presentValue -> new String[] {"-" + propertyName, presentValue}).orElse(null);
                })
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    public String[] getNodeIps() {
        return nodeIps;
    }

    public Integer getQueryNumber() {
        return queryNumber;
    }

    public File getInFile() {
        return inFile;
    }

    public File getOutFile() {
        return outFile;
    }

    public File getTimeFile() {
        return timeFile;
    }

    public Integer getN() {
        return n;
    }

    public Integer getProv() {
        return prov;
    }

    private static class CommaSeparator implements IStringConverter<String[]> {
        @Override
        public String[] convert(String value) {
            return value.split(",");
        }
    }

    private static class FileExistsValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if(!new File(value).exists()) {
                throw new ParameterException(name + "\"" + value + "\" does not exist");
            }
        }
    }
}

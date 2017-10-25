package ar.edu.itba;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.beust.jcommander.converters.InetAddressConverter;
import com.beust.jcommander.converters.IntegerConverter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

/**
 * POJO for run arguments. Serializable for parameters to be sent over the net to nodes.
 */
public class Arguments implements DataSerializable {

    private static final List<String> KNOWN_PROPERTIES = Arrays.asList("addresses", "query", "inPath", "outPath", "timeOutPath", "n", "prov");

    @Parameter(names = {"-addresses", "-a"}, description = "Node IP addresses", required = true, listConverter = CommaSeparator.class)
    private List<InetAddress> nodeIps;

    @Parameter(names = {"-query", "-q"}, description = "Query number to execute", required = true, converter = IntegerConverter.class)
    private Integer queryNumber;

    @Parameter(names = {"-inPath", "-i"}, description = "File from which to read data", required = true, converter = FileConverter.class, validateWith = FileExistsValidator.class)
    private File inFile;

    @Parameter(names = {"-outPath", "-o"}, description = "File to which to output result", required = true, converter = FileConverter.class)
    private File outFile;

    @Parameter(names = {"-timeOutPath", "-t"}, description = "File to which to output execution time", required = true, converter = FileConverter.class)
    private File timeFile;

    @Parameter(names = {"-n"}, converter = IntegerConverter.class)
    private Integer n;

    @Parameter(names = {"-prov"}, converter = IntegerConverter.class)
    private Integer prov;

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(nodeIps);
        objectDataOutput.writeInt(queryNumber);
        objectDataOutput.writeObject(inFile);
        objectDataOutput.writeObject(outFile);
        objectDataOutput.writeObject(timeFile);
        objectDataOutput.writeInt(n);
        objectDataOutput.writeInt(prov);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        nodeIps = objectDataInput.readObject();
        queryNumber = objectDataInput.readInt();
        inFile = objectDataInput.readObject();
        outFile = objectDataInput.readObject();
        timeFile = objectDataInput.readObject();
        n = objectDataInput.readInt();
        prov = objectDataInput.readInt();
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

    /**
     * Post validation, to be performed after initial schema validation.  Among other things, validates that the -n and
     * -prov parameters are used only when appropriate.
     *
     * @throws ParameterException If validation fails.
     */
    public void postValidate() throws ParameterException {
        if(queryNumber < 1 || queryNumber > 7) {
            throw new ParameterException("Query number must be between 1 and 7");
        }
        if(n != null) {
            if(queryNumber != 2 && queryNumber != 6 && queryNumber != 7) {
                throw new ParameterException("-n parameter may only be used with query numbers 2, 6, 7");
            }
        }
        if(prov != null && queryNumber != 2) {
            throw new ParameterException("-prov parameter may only be used with query number 2");
        }
    }

    public List<InetAddress> getNodeIps() {
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

    /**
     * Splits a single comma-separated string argument to various IP addresses.
     */
    private static class CommaSeparator implements IStringConverter<List<InetAddress>> {
        InetAddressConverter converter = new InetAddressConverter();

        @Override
        public List<InetAddress> convert(String line) {
            return Arrays.stream(line.split(",")).map(address -> converter.convert(address)).collect(Collectors.toList());
        }
    }

    /**
     * Validates that a file argument refers to a file that exists.
     */
    private static class FileExistsValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if(!new File(value).exists()) {
                throw new ParameterException(name + "\"" + value + "\" does not exist");
            }
        }
    }
}

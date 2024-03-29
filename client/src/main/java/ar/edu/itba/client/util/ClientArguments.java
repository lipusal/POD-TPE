package ar.edu.itba.client.util;

import ar.edu.itba.args.ColonSeparator;
import ar.edu.itba.args.FileExistsValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.beust.jcommander.converters.IntegerConverter;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * POJO for run arguments. Serializable for parameters to be sent over the net to nodes.
 */
public class ClientArguments {

    // Credentials
    public static final String GROUP_NAME = "53384-54197-54859-55824";
    public static final String WAIFUS = "Misa-Celty-Bulma-Taiga";

    public static final List<String> KNOWN_PROPERTIES = Arrays.asList("addresses", "query", "inPath", "outPath", "timeOutPath", "n", "prov");

    @Parameter(names = {"-addresses", "-a"}, description = "Node IP addresses", required = true, listConverter = ColonSeparator.class)
    private List<String> nodeIps;

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

    @Parameter(names = {"-prov"})
    private String prov;

    /**
     * Post validation, to be performed after initial schema validation.  Among other things, validates that the -n and
     * -prov parameters are used only when appropriate.
     *
     * @throws ParameterException If validation fails.
     */
    public void postValidate() throws ParameterException {
        if (queryNumber < 1 || queryNumber > 7) {
            throw new ParameterException("Query number must be between 1 and 7");
        }
        if(nRequired() && n == null) {
            throw new ParameterException("-n parameter is required for query # " + queryNumber);
        } else if(!nRequired() && n != null) {
            throw new ParameterException("-n parameter is not allowed for query #" + queryNumber);
        }
        if(provRequired() && prov == null) {
            throw new ParameterException("-prov parameter is required for query #" + queryNumber);
        } else if (!provRequired() && prov != null) {
            throw new ParameterException("-prov parameter is not allowed for query #" + queryNumber);
        }
    }

    /**
     * @return Whether -n parameter is required. NOTE: If not required and provided, will not pass validation.
     */
    private boolean nRequired() {
        return queryNumber == 2 || queryNumber == 6 || queryNumber == 7;
    }

    /**
     * @return Whether -prov parameter is required. NOTE: If not required and provided, will not pass validation.
     */
    private boolean provRequired() {
        return queryNumber == 2;
    }

    public List<String> getNodeIps() {
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

    public String getProv() {
        return prov;
    }
}

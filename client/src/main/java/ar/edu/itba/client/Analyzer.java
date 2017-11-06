package ar.edu.itba.client;

import ar.edu.itba.args.FileExistsValidator;
import ar.edu.itba.args.Util;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Automatic script that runs various queries with different parameters. Captures and saves time for each run.
 */
public class Analyzer {
    private static Logger logger = LoggerFactory.getLogger(Analyzer.class);

    private final AnalyzerArguments arguments;
    private final File baseDir;

    private Analyzer(AnalyzerArguments arguments) {
        this.arguments = arguments;
        baseDir = new File(arguments.getOutPath());
        // Create output directory if necessary
        if(!baseDir.exists() && !baseDir.mkdirs()) {
            System.out.println("Could not create output directory " + arguments.outPath + ". Aborting.");
            System.exit(1);
        }
    }

    public static void main(String[] cliArgs)  {
        // 1) Add system properties (-Dx=y)
        List<String> allArguments = Util.propertiesToArgs(System.getProperties(), AnalyzerArguments.KNOWN_PROPERTIES);
        // 2) Add program arguments
        allArguments.addAll(Arrays.asList(cliArgs));
        // 3) Parse and validate
        AnalyzerArguments args = new AnalyzerArguments();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(allArguments.toArray(new String[0]));

        Analyzer analyzer = new Analyzer(args);

        // Get all combinations of arguments and call client with specified args
        String[][] clientArgs = {
                // Query 1
                analyzer.buildParams(1),
                // Query 2
                analyzer.buildParams(2, 10, "Santa Fe"),
                analyzer.buildParams(2, 10, "Ciudad Autónoma de Buenos Aires"),
                analyzer.buildParams(2, 10, "Buenos Aires"),
                analyzer.buildParams(2, 10, "La Pampa"),
                analyzer.buildParams(2, 10, "Corrientes"),
                analyzer.buildParams(2, 10, "Tucumán"),
                analyzer.buildParams(2, 10, "Córdoba"),
                analyzer.buildParams(2, 10, "San Juan"),
                // Query 3
                analyzer.buildParams(3),
                // Query 4
                analyzer.buildParams(4),
                // Query 5
                analyzer.buildParams(5),
                // Query 6
                analyzer.buildParams(6, 2),
                analyzer.buildParams(6, 3),
                analyzer.buildParams(6, 4),
                analyzer.buildParams(6, 5),
                analyzer.buildParams(6, 6),
                analyzer.buildParams(6, 10),
                // Query 7
                analyzer.buildParams(7, 1),
                analyzer.buildParams(7, 2),
                analyzer.buildParams(7, 3),
                analyzer.buildParams(7, 4),
                analyzer.buildParams(7, 5),
        };

        for(String[] argGroup : clientArgs) {
            logger.info("Running with {}", (Object) argGroup);
            try {
                Client.main(argGroup);
            } catch (Exception e) {
                logger.warn("Exception during run: {}", e);
                logger.warn("Continuing with next run");
            }
            logger.info("------------------------------");
        }
        logger.info("***************************************");
        logger.info("DONE");
        logger.info("***************************************");
    }

    private String[] buildParams(int queryNumber) {
        String fileName = "q" + queryNumber;
        return new String[] {
                "-q", Integer.toString(queryNumber),
                "-a", arguments.getNodeIps(),
                "-i", toSafePath("" , arguments.getInFile().toString(), "" ),
                "-o", toSafePath(arguments.outPath, fileName, "_out.txt"),
                "-t", toSafePath(arguments.outPath, fileName, "_time.txt"),
        };
    }

    private String[] buildParams(int queryNumber, int n) {
        String fileName = "q" + queryNumber + "n" + n;
        return new String[] {
                "-q", Integer.toString(queryNumber),
                "-n", Integer.toString(n),
                "-a", arguments.getNodeIps(),
                "-i", toSafePath("" , arguments.getInFile().toString(), "" ),
                "-o", toSafePath(arguments.outPath, fileName, "_out.txt"),
                "-t", toSafePath(arguments.outPath, fileName, "_time.txt"),
        };
    }

    private String[] buildParams(int queryNumber, int n, String prov) {
        String fileName = "q" + queryNumber + "n" + n + "prov" + prov.replace(" ", "");
        return new String[] {
                "-q", Integer.toString(queryNumber),
                "-n", Integer.toString(n),
                "-prov", "\"" + prov + "\"",
                "-a", arguments.getNodeIps(),
                "-i", toSafePath("" , arguments.getInFile().toString(), "" ),
                "-o", toSafePath(arguments.outPath, fileName, "_out.txt"),
                "-t", toSafePath(arguments.outPath, fileName, "_time.txt"),
        };
    }

    private String toSafePath(String fileName, String suffix) {
        return toSafePath(arguments.getOutPath(), fileName, suffix);
    }

    private String toSafePath(String baseDir, String fileName, String suffix) {
        StringBuilder result = new StringBuilder();
        boolean surroundWithQuotes = !fileName.contains("\"") && !fileName.contains("'");
        if(surroundWithQuotes) {
            result.append("\"");
        }
        result.append(Paths.get(baseDir, fileName + suffix).toAbsolutePath().toString());
        if(surroundWithQuotes) {
            result.append("\"");
        }
        return result.toString();
    }

    private static class AnalyzerArguments {
        static final List<String> KNOWN_PROPERTIES = Arrays.asList("outDir", "o", "addresses", "a", "inPath", "i");

        @Parameter(names = {"-outDir", "-o"}, description = "Directory where to store output files.", required = true)
        private String outPath;

        @Parameter(names = {"-addresses", "-a"}, description = "Node IP addresses", required = true)
        private String nodeIps;

        @Parameter(names = {"-inPath", "-i"}, description = "File from which to read data", required = true, converter = FileConverter.class, validateWith = FileExistsValidator.class)
        private File inFile;

        String getOutPath() {
            return outPath;
        }

        String getNodeIps() {
            return nodeIps;
        }

        File getInFile() {
            return inFile;
        }
    }
}

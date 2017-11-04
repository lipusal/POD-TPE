package ar.edu.itba.server.util;

import ar.edu.itba.args.ColonSeparator;
import com.beust.jcommander.Parameter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ServerArguments {

    // Credentials
    public static final String GROUP_NAME = "53384-54197-54859-55824";

    public static final List<String> KNOWN_PROPERTIES = Arrays.asList("addresses", "a", "interface", "i");

    @Parameter(names = {"-addresses", "-a"}, description = "IP addresses of cluster members. This argument can be omitted for the first node to start the cluster.", required = false, listConverter = ColonSeparator.class)
    private List<String> nodeIps = Collections.emptyList();

    @Parameter(names = {"-interface", "-i"}, description = "Interface to listen on. Usually of the form 'x.y.z.*'", required = true)
    private String iface;

    public List<String> getNodeIps() {
        return nodeIps;
    }

    public String getInterface() {
        return iface;
    }
}

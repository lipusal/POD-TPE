package ar.edu.itba.server;

import ar.edu.itba.args.Util;
import com.beust.jcommander.JCommander;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        // Parse arguments
        ServerArguments parsedArgs = new ServerArguments();
        JCommander.newBuilder()
                .addObject(parsedArgs)
                .build()
                .parse(Util.propertiesToArgs(System.getProperties(), ServerArguments.KNOWN_PROPERTIES).toArray(new String[0]));

        logger.info("Node starting ...");

        Config config = new Config()
                .setGroupConfig(new GroupConfig()
                        .setName(ServerArguments.GROUP_NAME)
                        .setPassword(ServerArguments.GROUP_NAME)
                )
                .setNetworkConfig(new NetworkConfig()
                        .setJoin(new JoinConfig()
                                .setMulticastConfig(new MulticastConfig()
                                        .setEnabled(false))
                                .setTcpIpConfig(new TcpIpConfig()
                                        .setEnabled(true)
                                        .setMembers(parsedArgs.getNodeIps())
                                )
                        )
                        .setInterfaces(new InterfacesConfig()
                                .setEnabled(true)
                                .addInterface(parsedArgs.getInterface())
                        )
                );
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
    }
}

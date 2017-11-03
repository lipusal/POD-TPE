package ar.edu.itba.server;

import ar.edu.itba.Arguments;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("Node starting ...");

        Config config = new Config()
            .setGroupConfig(new GroupConfig()
                .setName(Arguments.GROUP_NAME)
                .setPassword(Arguments.GROUP_NAME)
            )
            .setNetworkConfig(new NetworkConfig()
                .setJoin(new JoinConfig()
                    .setMulticastConfig(new MulticastConfig()
                        .setEnabled(false))
                    .setTcpIpConfig(new TcpIpConfig()
                        .setEnabled(true)
                        .addMember("127.0.0.1")
                    )
                )
                .setInterfaces(new InterfacesConfig()
                    .setEnabled(true)
                    .addInterface("127.0.0.1")
                )
            );
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
    }
}

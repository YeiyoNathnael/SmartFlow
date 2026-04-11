package nathnael.abatye.adu.ac.ae;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.log.SysOutLogger;
import tech.kwik.core.server.ServerConnectionConfig;
import tech.kwik.core.server.ServerConnector;


public class Broker {


    // File that contains the server certificate
    private static final String KEYSTORE_FILE = "cert.jks";

    // Password used to open the keystore file
    private static final String KEYSTORE_PASSWORD = "secret";

    // Alias name of the certificate inside the keystore
    private static final String CERT_ALIAS = "servercert";
    
    
    
    
    public static void main(String[] args) throws Exception {

        final int PORT = Integer.parseInt(args[0]);
        final String topicRouterHost = args[1];
        final int topicRouterPort = Integer.parseInt(args[2]);
        final String brokerTopic = args[3];
        final String brokerAddress = args[4];

        registerWithTopicRouter(topicRouterHost, topicRouterPort, brokerTopic, brokerAddress);

        // Create a keystore object to store the server certificate
        KeyStore keyStore = KeyStore.getInstance("JKS");

        // Load the certificate file into the keystore
        try (FileInputStream fis = new FileInputStream(KEYSTORE_FILE)) {
            keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }

        // Create logger object for server logs
        SysOutLogger logger = new SysOutLogger();
        logger.logInfo(false);     // Disable info messages
        logger.logWarning(false);  // Disable warning messages

        // Set limits for incoming streams from clients
        ServerConnectionConfig config = ServerConnectionConfig.builder()
                .maxOpenPeerInitiatedBidirectionalStreams(50) // Allow max 50 two-way streams
                .maxOpenPeerInitiatedUnidirectionalStreams(0) // Do not allow one-way streams
                .build();

        // Build the QUIC server connector
        ServerConnector connector = ServerConnector.builder()
                .withPort(PORT) // Server will run on port 4433
                .withKeyStore(keyStore, CERT_ALIAS, KEYSTORE_PASSWORD.toCharArray())
                // Attach certificate for secure QUIC communication
                .withConfiguration(config) // Apply stream settings
                .withLogger(logger)        // Apply logger settings
                .build();

        // Register the application protocol and connection factory
        connector.registerApplicationProtocol(Alpn.PROTOCOL, new QuicProtocolFactory());

        // Start the QUIC server
        connector.start();

        // Print status messages to show server is running
        System.out.println("===============================================================");
        System.out.println("Topic: x Broker QUIC server started on port " + PORT);
        System.out.println("Listening for events...");
        System.out.println("--------------------------------------------------------------- ");
    }

        private static void registerWithTopicRouter(
            String topicRouterHost,
            int topicRouterPort,
            String brokerTopic,
            String brokerAddress) throws Exception {

        URI routerUri = URI.create("https://" + topicRouterHost + ":" + topicRouterPort);
        QuicClientConnection routerConnection = QuicClientConnection.newBuilder()
                .uri(routerUri)
                .applicationProtocol(Alpn.PROTOCOL)
                .noServerCertificateCheck()
                .build();

        routerConnection.connect();
        QuicStream stream = routerConnection.createStream(true);

        String routeAddress = brokerAddress;
        EventMessage routeRegistration = new EventMessage(brokerTopic, routeAddress, "broker", "broker");
        String json = routeRegistration.toJson() + "\n";

        OutputStream output = stream.getOutputStream();
        output.write(json.getBytes(StandardCharsets.UTF_8));
        output.flush();
        output.close();

        routerConnection.close();

        System.out.println("Registered broker route with TopicRouter -> topic=" + brokerTopic + ", address=" + routeAddress);
    }
}

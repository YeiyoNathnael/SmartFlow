package nathnael.abatye.adu.ac.ae;

import tech.kwik.core.log.SysOutLogger;
import tech.kwik.core.server.ServerConnectionConfig;
import tech.kwik.core.server.ServerConnector;

import java.io.FileInputStream;
import java.security.KeyStore;


public class Broker {


    // File that contains the server certificate
    private static final String KEYSTORE_FILE = "cert.jks";

    // Password used to open the keystore file
    private static final String KEYSTORE_PASSWORD = "secret";

    // Alias name of the certificate inside the keystore
    private static final String CERT_ALIAS = "servercert";
    
    
    
    
    public static void main(String[] args) throws Exception {

        
        final int PORT = Integer.parseInt(args[1]);

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
}

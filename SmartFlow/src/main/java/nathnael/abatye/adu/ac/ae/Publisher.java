package nathnael.abatye.adu.ac.ae;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicStream;

/**
 * Publisher acts as a city service generating events and sending them to the Event Broker over QUIC.
 * This implementation uses Kwik, a pure Java implementation of the QUIC protocol.
 */
public class Publisher {

    private final String brokerHost;
    private final int brokerPort;
    private final String publisherId;
    private final String type = "PUBLISHER";
    
    private QuicClientConnection connection;

    public Publisher(String brokerHost, int brokerPort, String publisherId) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.publisherId = publisherId;
    }

    /**
     * Initialize the QUIC connection to the Broker.
     */
    public void start() throws Exception {
        URI uri = URI.create("https://" + brokerHost + ":" + brokerPort);
        
        // Build the QUIC Client connection utilizing the 'smartflow' ALPN
        connection = QuicClientConnection.newBuilder()
                .uri(uri)
                .noServerCertificateCheck()
                .applicationProtocol(Alpn.PROTOCOL) // ALPN (Application-Layer Protocol Negotiation)
                .build();
                
        // Establishes the secure QUIC transport tunnel natively using Java
        connection.connect();
                
        System.out.println("[" + publisherId + "] Connected via QUIC at " + brokerHost + ":" + brokerPort);
    }

    /**
     * Publishes an event to the Event Broker by creating a unidirectional QUIC stream.
     * @param topic Topic of the event.
     * @param payload Payload describing the event.
     */
public void publish(String topic, String payload) throws Exception {
    EventMessage message = new EventMessage(topic, payload, type, publisherId);
    String json = message.toJson() + "\n";

    // Create a bidirectional stream
    QuicStream stream = connection.createStream(true);

    OutputStream output = stream.getOutputStream();
    InputStream input = stream.getInputStream();

    // ---- SEND ----
    output.write(json.getBytes(StandardCharsets.UTF_8));
    output.flush();
    output.close(); // IMPORTANT: signals end of sending (FIN)

    // ---- RECEIVE ----
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    String acknowledgement = reader.readLine();

    if (acknowledgement != null) {
        System.out.println("ACK from server: " + acknowledgement);
    } else {
        System.out.println("No ACK received (server closed stream)");
    }

    System.out.println("[" + publisherId + "] Successfully published event on topic: " + topic);
}
    /**
     * Terminates connection securely.
     */
    public void stop() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing publisher: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        Publisher publisher = new Publisher("127.0.0.1", 9999, "TrafficService-001");
        try {
            // Note: Since Broker is not implemented in this phase, starting this will fail to connect.
             publisher.start();
             publisher.publish("TRAFFIC.accidents", "Accident reported on Main Street, expected delay 15 mins");
        } finally {
            publisher.stop();
        }
    }
}

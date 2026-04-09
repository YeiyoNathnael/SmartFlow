package nathnael.abatye.adu.ac.ae;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Subscriber acts as a city application listening for real-time updates from the Event Broker over QUIC.
 * Uses Kwik, a pure Java implementation of QUIC.
 */
public class Subscriber {

    private final String brokerHost;
    private final int brokerPort;
    private final String subscriberId;
    
    private QuicClientConnection connection;
    private Thread listenerThread;

    public Subscriber(String brokerHost, int brokerPort, String subscriberId) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.subscriberId = subscriberId;
    }

    /**
     * Initializes QUIC Connection, opens a stream, sends the required topic subscription, and loops forever to receive events.
     */
    public void startAndSubscribe(String topic) throws Exception {
        URI uri = URI.create("https://" + brokerHost + ":" + brokerPort);
        
        connection = QuicClientConnection.newBuilder()
                .uri(uri)
                .applicationProtocol("smartflow")
                .build();
                
        // Establish QUIC connection
        connection.connect();
        System.out.println("[" + subscriberId + "] Connected to broker via QUIC at " + brokerHost + ":" + brokerPort);
        
        // Open a Bidirectional stream. We will write the SUBSCRIBE command, and continually read updates.
        QuicStream stream = connection.createStream(true);
        OutputStream output = stream.getOutputStream();
        
        String subCommand = "SUBSCRIBE " + topic + "\n";
        output.write(subCommand.getBytes(StandardCharsets.UTF_8));
        output.flush();
        System.out.println("[" + subscriberId + "] Sent subscription request for topic: " + topic);
        
        // Start a listener thread so it does not block the main process synchronously
        listenerThread = new Thread(() -> listenForEvents(stream));
        listenerThread.start();
    }
    
    private void listenForEvents(QuicStream stream) {
        try {
            InputStream input = stream.getInputStream();
            byte[] buffer = new byte[8192];
            int readBytes;
            
            // This loop stays active as long as the stream is open (event push model)
            while ((readBytes = input.read(buffer)) != -1) {
                String rawJson = new String(buffer, 0, readBytes, StandardCharsets.UTF_8);
                try {
                    EventMessage event = EventMessage.fromJson(rawJson.trim());
                    System.out.println("[" + subscriberId + "] Received Event: Topic=" + event.getTopic() 
                                     + " | Publisher=" + event.getPublisherId() 
                                     + " | Payload=" + event.getPayload());
                } catch (Exception e) {
                    System.out.println("[" + subscriberId + "] Received raw message: " + rawJson.trim());
                }
            }
        } catch (Exception e) {
            System.err.println("[" + subscriberId + "] Stream disconnected: " + e.getMessage());
        }
    }

    public void stop() {
        try {
            if (listenerThread != null && listenerThread.isAlive()) {
                listenerThread.interrupt();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing subscriber: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        Subscriber subscriber = new Subscriber("127.0.0.1", 9999, "AirportApp-001");
        try {
            // Note: Since Broker is not implemented in this phase, starting this will fail to connect.
            subscriber.startAndSubscribe("TRAFFIC.accidents");
            
        } finally {
            subscriber.stop();
        }
    }
}

package nathnael.abatye.adu.ac.ae;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import nathnael.abatye.adu.ac.ae.handlers.SecurityUtils;
import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicStream;

/**
 * Subscriber acts as a city application listening for real-time updates from the Event Broker over QUIC.
 * Uses Kwik, a pure Java implementation of QUIC.
 */
public class Subscriber {

    private final String brokerHost;
    private final int brokerPort;
    private final String subscriberId;
    private final String type = "SUBSCRIBER";
    
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
    public void start(String topic, String command) throws Exception {

        EventMessage request = new EventMessage(topic, command, type, subscriberId);
        String json = request.toJson()+"\n";
        
        URI uri = URI.create("https://" + brokerHost + ":" + brokerPort);
        
        connection = QuicClientConnection.newBuilder()
                .uri(uri)
                .applicationProtocol(Alpn.PROTOCOL)
                .noServerCertificateCheck()
                .build();
                
        // Establish QUIC connection
        connection.connect();
        System.out.println("[" + subscriberId + "] Connected via QUIC at " + brokerHost + ":" + brokerPort);
        
        // Open a Bidirectional stream. We will write the SUBSCRIBE command, and continually read updates.
        QuicStream stream = connection.createStream(true);
        OutputStream output = stream.getOutputStream();
        
        output.write(json.getBytes(StandardCharsets.UTF_8));
        output.flush();
        // Important: close write side so broker read loop sees end-of-request and can respond.
        output.close();
        System.out.println("[" + subscriberId + "] Sent " + command + " request for topic: " + topic);
        
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
                    String payloadForDisplay;
                    payloadForDisplay = SecurityUtils.decryptPayload(event.getPayload());
                    
                    System.out.println("[" + subscriberId + "] Received Event: Topic=" + event.getTopic()  
                                     + " | Payload=" + payloadForDisplay);
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
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("=== Subscriber CLI ===");
            System.out.println("Request format: SUBSCRIBE <topic>");
            System.out.println("Request format: UNSUBSCRIBE <topic>");
            System.out.println("Example: SUBSCRIBE TRAFFIC.accidents");
            System.out.println("Type EXIT to stop.");

            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine().trim();

                if (line.equalsIgnoreCase("EXIT")) {
                    break;
                }

                String[] parts = line.split("\\s+", 2);
                if (parts.length < 2) {
                    System.out.println("Invalid input. Use: SUBSCRIBE <topic> or UNSUBSCRIBE <topic>");
                    continue;
                }

                String command = parts[0].toUpperCase();
                String topic = parts[1].trim();

                try {
                    subscriber.start(topic, command);
                } catch (Exception e) {
                    System.err.println("Failed to send request: " + e.getMessage());
                }
            }
        } finally {
            subscriber.stop();
        }
    }
}

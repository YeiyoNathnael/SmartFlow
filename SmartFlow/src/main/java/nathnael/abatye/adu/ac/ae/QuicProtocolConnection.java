
package nathnael.yeiyo.adu.ac.ae;
import tech.kwik.core.QuicStream;
import tech.kwik.core.server.ApplicationProtocolConnection;

public class QuicProtocolConnection implements ApplicationProtocolConnection {

    private final int clientId;

    public QuicProtocolConnection(int clientId) {
        this.clientId = clientId; 
        // Store client ID (used to identify which client this connection belongs to)
    }

    @Override
    public void acceptPeerInitiatedStream(QuicStream stream) {
        // When a new stream arrives from the client

        Thread worker = new Thread(() -> handleStream(stream));
        // Create a new thread so each stream is handled independently (concurrent handling)

        worker.start(); 
        // Start processing the stream in parallel
    }

    private void handleStream(QuicStream stream) {
        try {
            // Read the full message sent by the client through the stream
            String message = MessageUtil.readAll(stream.getInputStream());

            // Identify what kind of message was received
            String messageType = MessageUtil.classifyMessage(message);

            // Print the received message, its type, and the stream ID
            System.out.println(message + " -> Received " + messageType 
                    + " | stream=" + stream.getInputStream());

            // Create an acknowledgment response to send back to the client
            String response = "ACK from Abu Dhabi Smart Mobility Control Center -> " + messageType + " received successfully";

            // Send the response back through the same stream
            MessageUtil.writeText(stream.getOutputStream(), response);

        } catch (Exception e) {
            // If an error happens while processing the stream
            System.err.println("Error handling traffic light stream: " + e.getMessage());

            try {
                // Reset the stream with an error code
                stream.resetStream(1);
            } catch (Exception ignored) {
                // Ignore reset errors
            }
        }
    }
}

package nathnael.abatye.adu.ac.ae;

import nathnael.abatye.adu.ac.ae.handlers.MessageUtil;
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


  // here to handle the connection, in both cases, if its a pub or sub
    private void handleStream(QuicStream stream) {
        try {
            // Read the full message sent by the client through the stream
            EventMessage message = MessageUtil.readAll(stream.getInputStream());

            // Identify what kind of message was received
            String messageType = message.getType();

            // Print the received message, its type, and the stream ID
            System.out.println(message + " -> Received from " + messageType 
                    + " | stream=" + stream.getInputStream());

            // Create an acknowledgment response to send back to the client
            String response;
            if (messageType == "SUBSCRIBER") {
               response =  "ACK from Broker -> request to " + message.getPayload()+ "to topic: " + message.getTopic()
              + " received successfully";             
            } else if(messageType == "PUBLISHER") {
              
              response =  "ACK from Broker -> " + message.getPayload()+ "to topic: " + message.getTopic()
              + " received successfully";

            }else{
                response = "ACK from Broker -> I DONT KNOW WHO YOU ARE";
            }
           
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

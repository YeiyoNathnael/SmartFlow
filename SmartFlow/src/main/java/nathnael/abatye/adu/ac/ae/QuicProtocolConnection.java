package nathnael.abatye.adu.ac.ae;

import java.util.Set;

import nathnael.abatye.adu.ac.ae.handlers.MessageUtil;
import nathnael.abatye.adu.ac.ae.handlers.NotifyWorker;
import nathnael.abatye.adu.ac.ae.handlers.PublisherHandler;
import nathnael.abatye.adu.ac.ae.handlers.StreamWorker;
import nathnael.abatye.adu.ac.ae.handlers.SubscriberHandler;
import tech.kwik.core.QuicStream;
import tech.kwik.core.server.ApplicationProtocolConnection;

public class QuicProtocolConnection implements ApplicationProtocolConnection {

    private final int clientId;
    private final PublisherHandler publishedEvents;
    private final SubscriberHandler subscribersPerTopic;


    public QuicProtocolConnection(int clientId, PublisherHandler publishedEvents, SubscriberHandler subscribersPerTopic) {
        this.clientId = clientId; 
        this.publishedEvents = publishedEvents;
        this.subscribersPerTopic = subscribersPerTopic;
        // Store client ID (used to identify which client this connection belongs to)

    }

    @Override
    public void acceptPeerInitiatedStream(QuicStream stream) {
        // When a new stream arrives from the client

        Thread worker = new Thread(new StreamWorker(this, stream));
        // Create a new thread so each stream is handled independently (concurrent handling)

        worker.start(); 
        // Start processing the stream in parallel
    }


  // here to handle the connection, in both cases, if its a pub or sub
    public void processStream(QuicStream stream) {
        try {
            // Read the full message sent by the client through the stream
            EventMessage message = MessageUtil.readAll(stream.getInputStream());

            // Identify what kind of message was received
            String messageType = message.getType();

            // Print the received message, its type, and the stream ID
                System.out.println("[Client-" + clientId + "] " + message + " -> Received from " + messageType
                    + " | stream=" + stream.getInputStream());

            // Create an acknowledgment response to send back to the client
            String response;
            switch (messageType) {
                case "SUBSCRIBER":
                    if ("SUBSCRIBE".equals(message.getPayload())) {
                        subscribersPerTopic.addSubscriber(message.getTopic(), stream);
                        response = "ACK from Broker -> request to " + message.getPayload() + " to topic: "
                                + message.getTopic() + " received successfully";
                        MessageUtil.writeTextNoClose(stream.getOutputStream(), response);
                        // Notify the subscriber immediately when connected/subscribed.
                        MessageUtil.writeTextNoClose(
                                stream.getOutputStream(),
                                "NOTIFY -> Subscribed to topic: " + message.getTopic());
                        return;
                    } else if ("UNSUBSCRIBE".equals(message.getPayload())) {
                        subscribersPerTopic.removeSubscriber(message.getTopic(), stream);
                        response = "ACK from Broker -> request to " + message.getPayload() + " to topic: "
                                + message.getTopic() + " received successfully";
                    } else {
                        response = "LITERALLY UNKNOWN REQUEST";
                    }
                    break;
                case "PUBLISHER":
                    // Queue publisher messages so subscribers can later receive them in topic FIFO order.
                    publishedEvents.queuePublishedEvent(message);
                    response = "ACK from Broker -> queued publish event to topic: " + message.getTopic();

                    EventMessage queuedEvent;
                    while ((queuedEvent = publishedEvents.getNextQueuedEvent(message.getTopic())) != null) {
                        final String eventTopic = queuedEvent.getTopic();
                        final String eventJson = queuedEvent.toJson();
                        Set<QuicStream> subscribers = subscribersPerTopic.getSubscribers(eventTopic);

                        for (QuicStream subscriberStream : subscribers) {
                            Thread notifierThread = new Thread(
                                    new NotifyWorker(this, eventTopic, eventJson, subscriberStream));
                            notifierThread.start();
                        }
                    }
                    break;
                default:
                    response = "ACK from Broker -> I DONT KNOW WHO YOU ARE";
                    break;
            }
            // Send the response back through the same stream
            MessageUtil.writeText(stream.getOutputStream(), response);

            // Now handle and do the main processing of the data.

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

    public void notifySubscriber(String topic, String eventJson, QuicStream subscriberStream) {
        try {
            MessageUtil.writeTextNoClose(subscriberStream.getOutputStream(), eventJson);
        } catch (Exception e) {
            subscribersPerTopic.removeSubscriber(topic, subscriberStream);
        }
    }
}

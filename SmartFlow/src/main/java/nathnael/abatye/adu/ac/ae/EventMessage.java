package nathnael.abatye.adu.ac.ae;

import com.google.gson.Gson;

/**
 * EventMessage encapsulates the structure of an event flowing through the SmartFlow system.
 * It contains the topic (e.g., TRAFFIC.accidents), the payload (data), and metadata like timestamp.
 */
public class EventMessage {
    private String topic;
    private String payload;
    private long timestamp;
    private String publisherId;

    public EventMessage() {}

    public EventMessage(String topic, String payload, String publisherId) {
        this.topic = topic;
        this.payload = payload;
        this.publisherId = publisherId;
        this.timestamp = System.currentTimeMillis();
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getPublisherId() {
        return publisherId;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static EventMessage fromJson(String json) {
        return new Gson().fromJson(json, EventMessage.class);
    }
}

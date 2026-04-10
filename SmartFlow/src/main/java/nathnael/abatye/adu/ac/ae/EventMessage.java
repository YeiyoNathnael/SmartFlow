package nathnael.abatye.adu.ac.ae;

import com.google.gson.Gson;

/**
 * EventMessage encapsulates the structure of an message flowing through the SmartFlow system.
 *
 * It can be understood from two perspectives, the Publisher and Subscriber
 * 
 * Publisher side:
 * It contains the type(Publisher) , topic (e.g., TRAFFIC.accidents), the payload (actual event message to send), and timestamp.
 * It will be understood as Publisher wants to send event message to this topic.
 *
 * Subscriber side:
 * It contains the type(Subscriber) , topic (e.g., TRAFFIC.accidents), the payload (SUBSCRIBE/UNSUBSCRIBE), and timestamp.
 * It will be understood as Subscriber wants to SUBSCRIBE or UNSUBSCRIBE to this topic.
 *
 */
public class EventMessage {
    
    private String type;
    private String topic;
    private String payload;
    private long timestamp;

    public EventMessage() {}

    public EventMessage(String topic, String payload, String type) {
        this.topic = topic;
        this.payload = payload;
        this.type = type;
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

    public String getType() {
        return type;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static EventMessage fromJson(String json) {
        return new Gson().fromJson(json, EventMessage.class);
    }
}

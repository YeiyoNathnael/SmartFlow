package nathnael.abatye.adu.ac.ae.handlers;

import nathnael.abatye.adu.ac.ae.QuicProtocolConnection;
import tech.kwik.core.QuicStream;

public final class NotifyWorker implements Runnable {

    private final QuicProtocolConnection connection;
    private final String topic;
    private final String eventJson;
    private final QuicStream subscriberStream;

    public NotifyWorker(
            QuicProtocolConnection connection,
            String topic,
            String eventJson,
            QuicStream subscriberStream) {
        this.connection = connection;
        this.topic = topic;
        this.eventJson = eventJson;
        this.subscriberStream = subscriberStream;
    }

    @Override
    public void run() {
        connection.notifySubscriber(topic, eventJson, subscriberStream);
    }
}

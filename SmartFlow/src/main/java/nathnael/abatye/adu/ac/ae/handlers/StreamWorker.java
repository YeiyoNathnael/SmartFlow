package nathnael.abatye.adu.ac.ae.handlers;

import nathnael.abatye.adu.ac.ae.QuicProtocolConnection;
import tech.kwik.core.QuicStream;

public final class StreamWorker implements Runnable {

    private final QuicProtocolConnection connection;
    private final QuicStream stream;

    public StreamWorker(QuicProtocolConnection connection, QuicStream stream) {
        this.connection = connection;
        this.stream = stream;
    }

    @Override
    public void run() {
        connection.processStream(stream);
    }
}

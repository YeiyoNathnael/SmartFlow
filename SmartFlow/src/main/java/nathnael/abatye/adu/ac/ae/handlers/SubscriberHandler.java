package nathnael.abatye.adu.ac.ae.handlers;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import tech.kwik.core.QuicStream;

public class SubscriberHandler {

	private final ConcurrentHashMap<String, Set<QuicStream>> subscriberStreams =
		new ConcurrentHashMap<>();

	public boolean addSubscriber(String topic, QuicStream stream) {
		validateTopic(topic);
		validateStream(stream);

		return subscriberStreams
			.computeIfAbsent(topic, key -> ConcurrentHashMap.newKeySet())
			.add(stream);
	}

	public boolean removeSubscriber(String topic, QuicStream stream) {
		validateTopic(topic);
		validateStream(stream);

		Set<QuicStream> streams = subscriberStreams.get(topic);
		if (streams == null) {
			return false;
		}

		boolean removed = streams.remove(stream);
		if (streams.isEmpty()) {
			subscriberStreams.remove(topic, streams);
		}
		return removed;
	}

	public Set<QuicStream> getSubscribers(String topic) {
		validateTopic(topic);

		Set<QuicStream> streams = subscriberStreams.get(topic);
		if (streams == null || streams.isEmpty()) {
			return Collections.emptySet();
		}

		return Collections.unmodifiableSet(new HashSet<>(streams));
	}

	private void validateTopic(String topic) {
		if (topic == null || topic.trim().isEmpty()) {
			throw new IllegalArgumentException("topic must not be null/blank");
		}
	}

	private void validateStream(QuicStream stream) {
		if (stream == null) {
			throw new IllegalArgumentException("stream must not be null");
		}
	}
}

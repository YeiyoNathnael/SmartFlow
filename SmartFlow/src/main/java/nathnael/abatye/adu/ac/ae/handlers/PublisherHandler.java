package nathnael.abatye.adu.ac.ae.handlers;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import nathnael.abatye.adu.ac.ae.EventMessage;


public class PublisherHandler {

 private final ConcurrentHashMap<String, Queue<EventMessage>> publishedEventsPerTopic =
		new ConcurrentHashMap<>();

	// Keep insertion order per topic via queue (FIFO).
	public void queuePublishedEvent(EventMessage event) {
		if (event == null || event.getTopic() == null || event.getTopic().equals("")) {
			throw new IllegalArgumentException("Event and topic must not be null/blank");
		}

		publishedEventsPerTopic
				.computeIfAbsent(event.getTopic(), key -> new ConcurrentLinkedQueue<>())
				.offer(event);
	}

	public EventMessage getNextQueuedEvent(String topic) {
		Queue<EventMessage> queue = publishedEventsPerTopic.get(topic);
		if (queue == null) {
			return null;
		}

		EventMessage next = queue.poll();
		if (queue.isEmpty()) {
			publishedEventsPerTopic.remove(topic, queue);
		}
		return next;
	}

	public EventMessage peekNextQueuedEvent(String topic) {
		Queue<EventMessage> queue = publishedEventsPerTopic.get(topic);
		return queue == null ? null : queue.peek();
	}

	public boolean hasQueuedEvents(String topic) {
		Queue<EventMessage> queue = publishedEventsPerTopic.get(topic);
		return queue != null && !queue.isEmpty();
	}

}

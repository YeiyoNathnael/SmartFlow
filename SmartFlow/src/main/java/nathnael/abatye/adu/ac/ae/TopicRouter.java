package nathnael.abatye.adu.ac.ae;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.ConcurrentHashMap;

import nathnael.abatye.adu.ac.ae.handlers.MessageUtil;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.log.SysOutLogger;
import tech.kwik.core.server.ApplicationProtocolConnection;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.core.server.ServerConnectionConfig;
import tech.kwik.core.server.ServerConnector;

public class TopicRouter {

	private static final String TOPIC_SEPARATOR = ".";
	private static final String KEYSTORE_FILE = "cert.jks";
	private static final String KEYSTORE_PASSWORD = "secret";
	private static final String CERT_ALIAS = "servercert";

	private final ConcurrentHashMap<String, String> routes = new ConcurrentHashMap<>();

	public TopicRouter() {
	}

	public void registerRoute(String topic, String brokerAddress) {
		routes.put(extractTopicGroup(topic), brokerAddress);
	}

	public boolean unregisterRoute(String topic) {
		return routes.remove(extractTopicGroup(topic)) != null;
	}

	public String resolveAddress(String topic) {
		String generalTopic = extractTopicGroup(topic);
		String address = routes.get(generalTopic);
		return address == null ? "broker not found" : address;
	}

	public void processRequest(QuicStream stream) throws Exception {
		EventMessage message = MessageUtil.readAll(stream.getInputStream());
		String messageType = message.getType();

		if ("broker".equalsIgnoreCase(messageType)) {
			registerRoute(message.getTopic(), message.getPayload());
			MessageUtil.writeText(stream.getOutputStream(), "ACK -> route added for " + extractTopicGroup(message.getTopic()));
			return;
		}

		if ("requester".equalsIgnoreCase(messageType)) {
			String resolvedAddress = resolveAddress(message.getTopic());
			MessageUtil.writeText(stream.getOutputStream(), resolvedAddress);
			return;
		}

		MessageUtil.writeText(stream.getOutputStream(), "ACK -> unknown request type");
	}

	private String extractTopicGroup(String topic) {
		int separatorIndex = topic.indexOf(TOPIC_SEPARATOR);
		if (separatorIndex <= 0) {
			return topic;
		}
		return topic.substring(0, separatorIndex).trim();
	}

	public static void main(String[] args) throws Exception {
		final int port = Integer.parseInt(args[0]);

		KeyStore keyStore = KeyStore.getInstance("JKS");
		try (FileInputStream fis = new FileInputStream(KEYSTORE_FILE)) {
			keyStore.load(fis, KEYSTORE_PASSWORD.toCharArray());
		}

		SysOutLogger logger = new SysOutLogger();
		logger.logInfo(false);
		logger.logWarning(false);

		ServerConnectionConfig config = ServerConnectionConfig.builder()
			.maxOpenPeerInitiatedBidirectionalStreams(50)
			.maxOpenPeerInitiatedUnidirectionalStreams(0)
			.build();

		ServerConnector connector = ServerConnector.builder()
			.withPort(port)
			.withKeyStore(keyStore, CERT_ALIAS, KEYSTORE_PASSWORD.toCharArray())
			.withConfiguration(config)
			.withLogger(logger)
			.build();

		TopicRouter router = new TopicRouter();
		connector.registerApplicationProtocol(Alpn.PROTOCOL, new TopicRouterProtocolFactory(router));
		connector.start();

		System.out.println("===============================================================");
		System.out.println("Topic Router QUIC server started on port " + port);
		System.out.println("Listening for topic route requests...");
		System.out.println("---------------------------------------------------------------");
	}

	private static final class TopicRouterProtocolFactory implements ApplicationProtocolConnectionFactory {

		private final TopicRouter router;

		private TopicRouterProtocolFactory(TopicRouter router) {
			this.router = router;
		}

		@Override
		public ApplicationProtocolConnection createConnection(String protocol, QuicConnection quicConnection) {
			return new TopicRouterProtocolConnection(router);
		}

		@Override
		public int maxConcurrentPeerInitiatedBidirectionalStreams() {
			return 50;
		}

		@Override
		public int maxConcurrentPeerInitiatedUnidirectionalStreams() {
			return 0;
		}
	}

	private static final class TopicRouterProtocolConnection implements ApplicationProtocolConnection {

		private final TopicRouter router;

		private TopicRouterProtocolConnection(TopicRouter router) {
			this.router = router;
		}

		@Override
		public void acceptPeerInitiatedStream(QuicStream stream) {
			Thread worker = new Thread(() -> {
				try {
					router.processRequest(stream);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
			worker.start();
		}
	}
}
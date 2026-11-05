# SmartFlow

SmartFlow is a topic-based publish/subscribe event system built on QUIC. Publishers send events to a broker, the broker validates and queues them per topic, and subscribers receive them over long-lived QUIC streams. A topic router maps topic groups to brokers, so subscribers locate the right broker by topic instead of by address.

The system is written in Java and uses [Kwik](https://github.com/ptrd/kwik) for the QUIC transport.

## Architecture

```
                 +--------------+
     register    | Topic Router |    resolve topic
   +------------>|              |<------------+
   |             +--------------+             |
   |                                          |
+--+-----+        publish event        +------+-----+
| Broker |<----------------------------| Publisher  |
|        |                             +------------+
|        |        event stream         +------------+
|        |---------------------------->| Subscriber |
+--------+                             +------------+
```

| Component | Class | Role |
|---|---|---|
| Topic Router | `TopicRouter` | Stores a route from each topic group to a broker address and answers lookups |
| Broker | `Broker` | Registers with the router, accepts publish and subscribe requests, and forwards events |
| Publisher | `Publisher` | Encrypts a payload and sends it to a broker, then reads the acknowledgement |
| Subscriber | `Subscriber` | Resolves a broker through the router, subscribes, and decrypts incoming events |

## How it works

### Routing

Topics use dotted names such as `TRAFFIC.accidents`. The segment before the first dot is the topic group. A broker registers the group it serves, and the router resolves any topic in that group to the broker's address. `TRAFFIC.accidents` and `TRAFFIC.signals` both resolve to the broker registered for `TRAFFIC`.

### Event delivery

1. The publisher encrypts the payload and opens a bidirectional stream to the broker.
2. The broker verifies the payload's authentication tag and rejects events that fail the check.
3. Valid events enter a FIFO queue for their topic.
4. The broker drains the queue and sends each event to every subscriber of that topic on its own worker thread.
5. The broker removes a subscriber if a write to its stream fails.

### Concurrency

Each incoming stream is handled on its own thread. Topic queues and subscriber sets use `ConcurrentHashMap` and `ConcurrentLinkedQueue`. Servers accept up to 50 concurrent bidirectional streams per connection.

### Security

- Transport: QUIC, which uses TLS 1.3. Servers load their certificate from the `cert.jks` keystore.
- Payloads: AES-GCM with a 96-bit random IV and a 128-bit authentication tag. Publishers, brokers and subscribers share a key file.
- Application protocol: clients and servers negotiate the ALPN identifier `smart-flow-v1`.

## Message format

Every message is a single line of JSON:

```json
{
  "type": "PUBLISHER",
  "topic": "TRAFFIC.accidents",
  "payload": "<base64 IV and ciphertext>",
  "timestamp": 1775952000000,
  "id": "TrafficService-001"
}
```

| Field | Description |
|---|---|
| `type` | `PUBLISHER`, `SUBSCRIBER`, `broker` or `requester` |
| `topic` | Dotted topic name |
| `payload` | Encrypted event, `SUBSCRIBE`/`UNSUBSCRIBE`, or a broker address when registering a route |
| `timestamp` | Creation time in milliseconds since the Unix epoch |
| `id` | Sender identifier |

## Getting started

### Requirements

- Java 21
- Maven 3.9

### Build

```bash
cd SmartFlow
mvn compile dependency:build-classpath -Dmdep.outputFile=cp.txt
```

### Create the shared key

```bash
mkdir -p src/main/resources
openssl rand -base64 32 > src/main/resources/shared_aes.key
```

### Run

Run each component in its own terminal from the `SmartFlow` directory.

```bash
CP="target/classes:$(cat cp.txt)"
PKG=nathnael.abatye.adu.ac.ae

# Terminal 1: topic router on port 7000
java -cp "$CP" $PKG.TopicRouter 7000

# Terminal 2: broker for the TRAFFIC topic group on port 9999
java -cp "$CP" $PKG.Broker 9999 127.0.0.1 7000 TRAFFIC 127.0.0.1:9999

# Terminal 3: subscriber
java -cp "$CP" $PKG.Subscriber 127.0.0.1 7000 sub-1
> SUBSCRIBE TRAFFIC.accidents

# Terminal 4: publisher, which sends a sample event to 127.0.0.1:9999
java -cp "$CP" $PKG.Publisher
```

The subscriber prints the decrypted event:

```text
[sub-1] Received Event: Topic=TRAFFIC.accidents | Payload=Accident reported on Main Street, expected delay 15 mins
```

### Command-line arguments

| Component | Arguments |
|---|---|
| `TopicRouter` | `<port>` |
| `Broker` | `<port> <routerHost> <routerPort> <topicGroup> <brokerAddress>` |
| `Subscriber` | `<routerHost> <routerPort> <subscriberId>` |

The subscriber accepts `SUBSCRIBE <topic>`, `UNSUBSCRIBE <topic>` and `EXIT` on standard input.

## Project layout

```text
SmartFlow/
  cert.jks                       Server keystore
  pom.xml
  src/main/java/nathnael/abatye/adu/ac/ae/
    Alpn.java                    ALPN identifier
    Broker.java                  Broker entry point
    EventMessage.java            Message model and JSON mapping
    Publisher.java               Publisher client
    QuicProtocolConnection.java  Stream handling and delivery
    QuicProtocolFactory.java     Connection factory and shared state
    Subscriber.java              Subscriber client
    TopicRouter.java             Route registry and lookup server
    handlers/
      MessageUtil.java           Stream reads and writes
      NotifyWorker.java          Subscriber delivery task
      PublisherHandler.java      Per-topic FIFO queues
      SecurityUtils.java         AES-GCM encryption and verification
      StreamWorker.java          Incoming stream task
      SubscriberHandler.java     Per-topic subscriber sets
```

## Built with

- [Kwik](https://github.com/ptrd/kwik) 0.10.8
- [Gson](https://github.com/google/gson) 2.13.2

## Authors

- Nathnael Workneh Yeiyo

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build all modules
./gradlew build

# Compile without running tests
./gradlew compileJava

# Run a specific demo class (from repo root)
./gradlew :kafka-basics:run -PmainClass=com.luongnm93.ProducerDemo

# Run tests
./gradlew test
```

To run a class directly (e.g. for quick iteration), use the Gradle application plugin or run the compiled `.class` from `kafka-basics/build/classes/java/main/`.

## Architecture

This is a Gradle multi-module project with a single submodule:

- **`kafka-basics/`** — standalone Java demos for the Kafka Java client (no Spring Boot). Each class has its own `main()` and is meant to be run independently.

### Demo classes in `kafka-basics/src/main/java/com/luongnm93/`

| Class | Purpose |
|---|---|
| `ProducerDemo` | Minimal producer — sends one message to topic `demo_java` |
| `ProducerDemoWithCallback` | Producer with send callback for async acknowledgement |
| `ProducerDemoWithMessageKey` | Producer that sets explicit message keys (controls partition routing) |
| `ConsumerDemo` | Basic consumer loop on topic `demo_java`, group `my-java-application` |
| `ConsumerDemoWithShutdown` | Consumer with graceful shutdown via `Runtime.addShutdownHook` |

### Key configuration

- Broker: `127.0.0.1:9092` (local Kafka required)
- Topic: `demo_java`
- Consumer group: `my-java-application`
- `AUTO_OFFSET_RESET_CONFIG = "earliest"` — consumers start from the beginning when no committed offset exists

### Dependencies

- `org.apache.kafka:kafka-clients:4.2.0`
- `org.slf4j:slf4j-simple:2.0.17` (logging to stdout)

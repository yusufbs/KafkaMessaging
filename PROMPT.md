# Kafka Messaging Solution Generator Prompt

Use the prompt below with any AI coding assistant to generate this solution from scratch:

---

```markdown
Create a complete, production-ready .NET 8/9 C# solution for Apache Kafka publishing and consuming using `Confluent.Kafka`.

### Solution & Project Structure
Create a solution named `KafkaMessaging.sln` with the following clean architecture project structure:
1. `KafkaMessaging.Publisher` (Class Library):
   - Configurable Kafka producer wrapper.
   - Strongly-typed configuration class (`KafkaPublisherOptions`) bindable from `appsettings.json`.
   - Generic publisher interface `IKafkaPublisher<TKey, TValue>` and implementation supporting asynchronous produce operations (`ProduceAsync`) with message headers, partition key support, and delivery result handling.
   - Built-in retry/resilience and cancellation token support.
   - Extensible serialization (JSON by default using `System.Text.Json`).
   - Dependency Injection extensions (`IServiceCollection.AddKafkaPublisher(...)`).

2. `KafkaMessaging.Consumer` (Class Library):
   - Configurable Kafka consumer wrapper and background worker (`BackgroundService`).
   - Strongly-typed configuration class (`KafkaConsumerOptions`) including: `BootstrapServers`, `GroupId`, `Topic`, `AutoOffsetReset`, `EnableAutoCommit`, `MaxPollIntervalMs`, `SessionTimeoutMs`, etc.
   - Typed message handler interface `IKafkaMessageHandler<TKey, TValue>`.
   - Robust consuming loop with manual/auto commit strategies, graceful shutdown via `CancellationToken`, error handling, and dead-letter/logging hook.
   - Extensible deserialization (JSON by default).
   - Dependency Injection extensions (`IServiceCollection.AddKafkaConsumer<TKey, TValue, THandler>(...)`).

3. `KafkaMessaging.Publisher.Sample` (Console/Worker Application):
   - Demonstrates DI registration via `Host.CreateApplicationBuilder()`.
   - Reads Kafka settings from `appsettings.json`.
   - Publishes strongly-typed test messages (e.g., `OrderCreatedEvent`) to a configured topic at intervals or on demand.

4. `KafkaMessaging.Consumer.Sample` (Worker Application):
   - Demonstrates DI registration of the consumer background service and a custom `OrderCreatedEventHandler`.
   - Reads consumer settings from `appsettings.json`.
   - Processes consumed messages and logs offsets, keys, and payload contents.

### Technical Requirements & Best Practices
- **NuGet Packages**: Use `Confluent.Kafka`, `Microsoft.Extensions.Hosting`, `Microsoft.Extensions.Options.ConfigurationExtensions`, `Microsoft.Extensions.Logging.Abstractions`, and `System.Text.Json`.
- **Reliability & Idempotence**: Enable idempotent delivery (`EnableIdempotence = true`) and `Acks = Acks.All` by default in publisher settings, but make all producer/consumer config parameters overrideable via configuration dictionaries.
- **Error Handling**: Graceful handling of `ProduceException`, `ConsumeException`, and network drops with structured logging via `ILogger`.
- **Configuration Samples**: Provide complete `appsettings.json` samples for both sample apps.
- **Setup Guide**: Include dotnet CLI commands to build and run both projects, plus an optional Docker Compose snippet for quickly spinning up Kafka and Zookeeper / KRaft locally.
- Write full, working C# code for all files without placeholders or omitted implementations.
```

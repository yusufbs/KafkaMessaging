# KafkaMessaging (.NET 8/9 C#)

Configurable, resilient, strongly-typed Apache Kafka Publisher and Consumer libraries with sample projects.

## Project Structure

```
KafkaMessaging/
├── src/
│   ├── KafkaMessaging.Publisher/        # Configurable Kafka Producer Library
│   └── KafkaMessaging.Consumer/         # Configurable Kafka Consumer & Background Service Library
├── samples/
│   ├── KafkaMessaging.Publisher.Sample/ # Sample console producer dispatching OrderCreatedEvent
│   └── KafkaMessaging.Consumer.Sample/  # Sample worker consumer handling OrderCreatedEvent
├── docker-compose.yml                   # Local single-node Kafka (KRaft mode)
└── KafkaMessaging.sln                   # Visual Studio Solution
```

---

## 🚀 Quick Start

### 1. Start Local Kafka (KRaft)
```bash
docker compose up -d
```
This spins up Apache Kafka in KRaft mode listening on `localhost:9092` with 3 partitions per topic.

### 2. Run the Consumer Sample
In a terminal, start the consumer worker:
```bash
dotnet run --project samples/KafkaMessaging.Consumer.Sample
```

### 3. Run the Publisher Sample
In a second terminal, execute the publisher:
```bash
dotnet run --project samples/KafkaMessaging.Publisher.Sample
```
You will see 5 orders published by the publisher and immediately picked up, deserialized, and processed by the consumer handler.

---

## 📦 Publisher Library (`KafkaMessaging.Publisher`)

### Configuration (`appsettings.json`)
```json
{
  "Kafka": {
    "Publisher": {
      "BootstrapServers": "localhost:9092",
      "ClientId": "order-publisher",
      "DefaultTopic": "orders-topic",
      "Acks": "All",
      "EnableIdempotence": true,
      "MessageTimeoutMs": 10000,
      "CompressionType": "Snappy"
    }
  }
}
```

### Dependency Injection Registration
```csharp
// Program.cs
builder.Services.AddKafkaPublisher<string, OrderCreatedEvent>(builder.Configuration);
```

### Producing Messages
```csharp
public class OrderService
{
    private readonly IKafkaPublisher<string, OrderCreatedEvent> _publisher;

    public OrderService(IKafkaPublisher<string, OrderCreatedEvent> publisher)
    {
        _publisher = publisher;
    }

    public async Task CreateOrderAsync(OrderCreatedEvent order, CancellationToken ct)
    {
        var headers = new Headers
        {
            { "correlation-id", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
        };

        // Publishes to default topic using order.OrderId as key
        await _publisher.ProduceAsync(order.OrderId.ToString(), order, headers, ct);
    }
}
```

---

## 📥 Consumer Library (`KafkaMessaging.Consumer`)

### Configuration (`appsettings.json`)
```json
{
  "Kafka": {
    "Consumer": {
      "BootstrapServers": "localhost:9092",
      "GroupId": "order-processing-group",
      "Topic": "orders-topic",
      "AutoOffsetReset": "Earliest",
      "EnableAutoCommit": false,
      "SessionTimeoutMs": 45000,
      "MaxPollIntervalMs": 300000
    }
  }
}
```

### Creating a Message Handler
Handlers are registered in DI and executed in a dedicated DI scope per message:
```csharp
public class OrderCreatedEventHandler : IKafkaMessageHandler<string, OrderCreatedEvent>
{
    private readonly ILogger<OrderCreatedEventHandler> _logger;

    public OrderCreatedEventHandler(ILogger<OrderCreatedEventHandler> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(ConsumeResult<string, OrderCreatedEvent> message, CancellationToken cancellationToken)
    {
        var order = message.Message.Value;
        _logger.LogInformation("Processing order: {OrderId} for {Customer}", order.OrderId, order.CustomerName);
        return Task.CompletedTask;
    }

    public Task OnErrorAsync(Exception ex, ConsumeResult<string, OrderCreatedEvent>? message, CancellationToken cancellationToken)
    {
        _logger.LogError(ex, "Error processing message");
        return Task.CompletedTask;
    }
}
```

### Dependency Injection Registration
```csharp
// Program.cs
builder.Services.AddKafkaConsumer<string, OrderCreatedEvent, OrderCreatedEventHandler>(builder.Configuration);
```

---

## ⚙️ Features
- **Idempotence & At-least-once Guarantee**: Publisher defaults to `Acks.All` and `EnableIdempotence = true`. Consumer supports manual commit after successful handler completion.
- **Scoped DI Lifecycles**: Each consumed message runs inside its own `IServiceScope`, enabling clean usage of scoped database contexts (e.g. EF Core `DbContext`).
- **Flexible Serialization**: Default JSON serialization with `System.Text.Json`, supporting custom `ISerializer<T>` / `IDeserializer<T>`.
- **Structured Logging**: Built-in logging with `ILogger` for error conditions, broker warnings, partition assignments, and delivery results.

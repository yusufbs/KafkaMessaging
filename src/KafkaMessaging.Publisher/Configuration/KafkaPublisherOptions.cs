using Confluent.Kafka;

namespace KafkaMessaging.Publisher.Configuration;

/// <summary>
/// Configuration options for Kafka publisher.
/// Can be bound from appsettings.json.
/// </summary>
public class KafkaPublisherOptions
{
    public const string SectionName = "Kafka:Publisher";

    /// <summary>
    /// Initial list of brokers as a CSV list of broker host or host:port.
    /// </summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Client identifier string to pass to the broker in requests.
    /// </summary>
    public string ClientId { get; set; } = "kafka-publisher";

    /// <summary>
    /// Default topic to publish messages to if not specified in ProduceAsync.
    /// </summary>
    public string? DefaultTopic { get; set; } = "orders-topic";

    /// <summary>
    /// Acknowledgement mode: None (0), Leader (1), All (-1).
    /// Defaults to All for reliable delivery.
    /// </summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>
    /// When set to true, the producer will ensure that exactly one copy of each message is written.
    /// Default is true.
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Local message timeout in milliseconds.
    /// </summary>
    public int? MessageTimeoutMs { get; set; } = 15000;

    /// <summary>
    /// How many times to retry sending a failing message.
    /// </summary>
    public int? MessageSendMaxRetries { get; set; } = 5;

    /// <summary>
    /// Backoff in milliseconds before retrying message send.
    /// </summary>
    public int? RetryBackoffMs { get; set; } = 250;

    /// <summary>
    /// Delay in milliseconds to wait for messages in the producer queue to accumulate before sending.
    /// </summary>
    public double? LingerMs { get; set; } = 5;

    /// <summary>
    /// Compression codec to use for compressing message sets.
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.Snappy;

    /// <summary>
    /// Additional librdkafka configuration properties.
    /// </summary>
    public Dictionary<string, string> AdditionalConfig { get; set; } = new();

    /// <summary>
    /// Converts these options to Confluent.Kafka ProducerConfig.
    /// </summary>
    public ProducerConfig ToProducerConfig()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            ClientId = ClientId,
            Acks = Acks,
            EnableIdempotence = EnableIdempotence,
            CompressionType = CompressionType
        };

        if (MessageTimeoutMs.HasValue)
            config.MessageTimeoutMs = MessageTimeoutMs.Value;

        if (MessageSendMaxRetries.HasValue)
            config.MessageSendMaxRetries = MessageSendMaxRetries.Value;

        if (RetryBackoffMs.HasValue)
            config.RetryBackoffMs = RetryBackoffMs.Value;

        if (LingerMs.HasValue)
            config.LingerMs = LingerMs.Value;

        foreach (var (key, value) in AdditionalConfig)
        {
            config.Set(key, value);
        }

        return config;
    }
}

using Confluent.Kafka;

namespace KafkaMessaging.Consumer.Configuration;

/// <summary>
/// Configuration options for Kafka consumer.
/// Can be bound from appsettings.json.
/// </summary>
public class KafkaConsumerOptions
{
    public const string SectionName = "Kafka:Consumer";

    /// <summary>
    /// Initial list of brokers as a CSV list of broker host or host:port.
    /// </summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Consumer group id string. All consumers sharing the same GroupId form a consumer group.
    /// </summary>
    public string GroupId { get; set; } = "default-consumer-group";

    /// <summary>
    /// Single target topic to subscribe to.
    /// </summary>
    public string? Topic { get; set; } = "orders-topic";

    /// <summary>
    /// Optional list of multiple topics to subscribe to.
    /// </summary>
    public List<string> Topics { get; set; } = new();

    /// <summary>
    /// Action to take when there is no initial offset in offset store or the desired offset is out of range:
    /// Earliest, Latest, Error. Default is Earliest.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary>
    /// Automatically and periodically commit offsets in the background. Default is false (manual commit after handle).
    /// </summary>
    public bool EnableAutoCommit { get; set; } = false;

    /// <summary>
    /// The frequency in milliseconds that the consumer offsets are committed (if EnableAutoCommit = true).
    /// </summary>
    public int? AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Client group session and failure detection timeout.
    /// </summary>
    public int? SessionTimeoutMs { get; set; } = 45000;

    /// <summary>
    /// Maximum allowed time between calls to consume messages before the consumer is considered unresponsive.
    /// </summary>
    public int? MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Emit EOF event when reaching the end of a partition.
    /// </summary>
    public bool EnablePartitionEof { get; set; } = false;

    /// <summary>
    /// Allow automatic topic creation on the broker.
    /// </summary>
    public bool AllowAutoCreateTopics { get; set; } = true;

    /// <summary>
    /// Additional librdkafka configuration properties.
    /// </summary>
    public Dictionary<string, string> AdditionalConfig { get; set; } = new();

    /// <summary>
    /// Gets all configured topics combined.
    /// </summary>
    public IEnumerable<string> GetConfiguredTopics()
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(Topic))
        {
            result.Add(Topic.Trim());
        }

        foreach (var t in Topics.Where(t => !string.IsNullOrWhiteSpace(t)))
        {
            result.Add(t.Trim());
        }

        return result;
    }

    /// <summary>
    /// Converts these options to Confluent.Kafka ConsumerConfig.
    /// </summary>
    public ConsumerConfig ToConsumerConfig()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset,
            EnableAutoCommit = EnableAutoCommit,
            EnablePartitionEof = EnablePartitionEof,
            AllowAutoCreateTopics = AllowAutoCreateTopics
        };

        if (AutoCommitIntervalMs.HasValue)
            config.AutoCommitIntervalMs = AutoCommitIntervalMs.Value;

        if (SessionTimeoutMs.HasValue)
            config.SessionTimeoutMs = SessionTimeoutMs.Value;

        if (MaxPollIntervalMs.HasValue)
            config.MaxPollIntervalMs = MaxPollIntervalMs.Value;

        foreach (var (key, value) in AdditionalConfig)
        {
            config.Set(key, value);
        }

        return config;
    }
}

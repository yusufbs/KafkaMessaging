using Confluent.Kafka;
using KafkaMessaging.Publisher.Configuration;
using KafkaMessaging.Publisher.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaMessaging.Publisher;

/// <summary>
/// Thread-safe generic Kafka publisher with retry handling, logging, and metrics.
/// </summary>
public class KafkaPublisher<TKey, TValue> : IKafkaPublisher<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producer;
    private readonly KafkaPublisherOptions _options;
    private readonly ILogger<KafkaPublisher<TKey, TValue>> _logger;
    private bool _disposed;

    public KafkaPublisher(
        IOptions<KafkaPublisherOptions> options,
        ILogger<KafkaPublisher<TKey, TValue>> logger,
        ISerializer<TKey>? keySerializer = null,
        ISerializer<TValue>? valueSerializer = null)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var producerConfig = _options.ToProducerConfig();
        var builder = new ProducerBuilder<TKey, TValue>(producerConfig);

        // Key serializer setup
        if (keySerializer != null)
        {
            builder.SetKeySerializer(keySerializer);
        }
        else if (typeof(TKey) == typeof(string))
        {
            builder.SetKeySerializer((ISerializer<TKey>)Serializers.Utf8);
        }
        else if (typeof(TKey) == typeof(Null))
        {
            builder.SetKeySerializer((ISerializer<TKey>)Serializers.Null);
        }
        else
        {
            builder.SetKeySerializer(new DefaultJsonSerializer<TKey>());
        }

        // Value serializer setup
        if (valueSerializer != null)
        {
            builder.SetValueSerializer(valueSerializer);
        }
        else if (typeof(TValue) == typeof(string))
        {
            builder.SetValueSerializer((ISerializer<TValue>)Serializers.Utf8);
        }
        else if (typeof(TValue) == typeof(byte[]))
        {
            builder.SetValueSerializer((ISerializer<TValue>)Serializers.ByteArray);
        }
        else
        {
            builder.SetValueSerializer(new DefaultJsonSerializer<TValue>());
        }

        builder.SetErrorHandler((_, error) =>
        {
            if (error.IsFatal)
            {
                _logger.LogCritical("Fatal Kafka Producer error: {Reason} (Code: {Code})", error.Reason, error.Code);
            }
            else
            {
                _logger.LogWarning("Kafka Producer warning/error: {Reason} (Code: {Code})", error.Reason, error.Code);
            }
        });

        builder.SetLogHandler((_, logMessage) =>
        {
            _logger.LogDebug("[librdkafka {Facility}] {Message}", logMessage.Facility, logMessage.Message);
        });

        _producer = builder.Build();
    }

    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        TKey key,
        TValue value,
        Headers? headers = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic))
            throw new ArgumentException("Topic must not be null or whitespace.", nameof(topic));

        var message = new Message<TKey, TValue>
        {
            Key = key,
            Value = value,
            Headers = headers ?? new Headers(),
            Timestamp = new Timestamp(DateTime.UtcNow)
        };

        try
        {
            _logger.LogDebug("Sending message to Kafka topic '{Topic}' with key '{Key}'", topic, key);
            var result = await _producer.ProduceAsync(topic, message, cancellationToken);
            _logger.LogInformation(
                "Successfully published message to '{Topic}' [Partition: {Partition}, Offset: {Offset}, Status: {Status}]",
                result.Topic,
                result.Partition.Value,
                result.Offset.Value,
                result.Status);

            return result;
        }
        catch (ProduceException<TKey, TValue> ex)
        {
            _logger.LogError(
                ex,
                "Failed to deliver message to Kafka topic '{Topic}'. Code: {Code}, Reason: {Reason}",
                topic,
                ex.Error.Code,
                ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error producing message to Kafka topic '{Topic}'", topic);
            throw;
        }
    }

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        TKey key,
        TValue value,
        Headers? headers = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_options.DefaultTopic))
        {
            throw new InvalidOperationException(
                "DefaultTopic is not configured in KafkaPublisherOptions. " +
                "Either configure 'Kafka:Publisher:DefaultTopic' in appsettings.json, " +
                "set DefaultTopic in options, or call ProduceAsync(topic, key, value).");
        }

        return ProduceAsync(_options.DefaultTopic, key, value, headers, cancellationToken);
    }

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        TValue value,
        Headers? headers = null,
        CancellationToken cancellationToken = default)
    {
        return ProduceAsync(topic, default!, value, headers, cancellationToken);
    }

    public void Flush(TimeSpan timeout)
    {
        _producer.Flush(timeout);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Exception while disposing Kafka producer.");
        }
    }
}

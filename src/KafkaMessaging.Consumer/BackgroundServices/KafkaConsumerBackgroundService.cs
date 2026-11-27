using Confluent.Kafka;
using KafkaMessaging.Consumer.Configuration;
using KafkaMessaging.Consumer.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaMessaging.Consumer.BackgroundServices;

/// <summary>
/// Long-running background service that manages consuming loop, message dispatch to scoped handler,
/// offset commit strategies, and rebalancing.
/// </summary>
public class KafkaConsumerBackgroundService<TKey, TValue> : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly KafkaConsumerOptions _options;
    private readonly ILogger<KafkaConsumerBackgroundService<TKey, TValue>> _logger;
    private readonly IDeserializer<TKey>? _keyDeserializer;
    private readonly IDeserializer<TValue>? _valueDeserializer;

    public KafkaConsumerBackgroundService(
        IServiceProvider serviceProvider,
        IOptions<KafkaConsumerOptions> options,
        ILogger<KafkaConsumerBackgroundService<TKey, TValue>> logger,
        IDeserializer<TKey>? keyDeserializer = null,
        IDeserializer<TValue>? valueDeserializer = null)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _keyDeserializer = keyDeserializer;
        _valueDeserializer = valueDeserializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Yield thread so application startup completes smoothly before entering loop
        await Task.Yield();

        var topics = _options.GetConfiguredTopics().ToList();
        if (!topics.Any())
        {
            _logger.LogError("Kafka Consumer started without any configured topics. Shutting down consumer worker.");
            return;
        }

        var config = _options.ToConsumerConfig();
        var builder = new ConsumerBuilder<TKey, TValue>(config);

        // Key Deserializer setup
        if (_keyDeserializer != null)
        {
            builder.SetKeyDeserializer(_keyDeserializer);
        }
        else if (typeof(TKey) == typeof(string))
        {
            builder.SetKeyDeserializer((IDeserializer<TKey>)Deserializers.Utf8);
        }
        else if (typeof(TKey) == typeof(Null))
        {
            builder.SetKeyDeserializer((IDeserializer<TKey>)Deserializers.Null);
        }
        else
        {
            builder.SetKeyDeserializer(new DefaultJsonDeserializer<TKey>());
        }

        // Value Deserializer setup
        if (_valueDeserializer != null)
        {
            builder.SetValueDeserializer(_valueDeserializer);
        }
        else if (typeof(TValue) == typeof(string))
        {
            builder.SetValueDeserializer((IDeserializer<TValue>)Deserializers.Utf8);
        }
        else if (typeof(TValue) == typeof(byte[]))
        {
            builder.SetValueDeserializer((IDeserializer<TValue>)Deserializers.ByteArray);
        }
        else
        {
            builder.SetValueDeserializer(new DefaultJsonDeserializer<TValue>());
        }

        builder.SetErrorHandler((_, error) =>
        {
            if (error.IsFatal)
            {
                _logger.LogCritical("Fatal Kafka Consumer error: {Reason} (Code: {Code})", error.Reason, error.Code);
            }
            else
            {
                _logger.LogWarning("Kafka Consumer warning: {Reason} (Code: {Code})", error.Reason, error.Code);
            }
        });

        builder.SetPartitionsAssignedHandler((c, partitions) =>
        {
            _logger.LogInformation("Assigned partitions: [{Partitions}]",
                string.Join(", ", partitions.Select(p => $"{p.Topic}:{p.Partition.Value}")));
        });

        builder.SetPartitionsRevokedHandler((c, partitions) =>
        {
            _logger.LogInformation("Revoked partitions: [{Partitions}]",
                string.Join(", ", partitions.Select(p => $"{p.Topic}:{p.Partition.Value}")));
        });

        using var consumer = builder.Build();

        try
        {
            consumer.Subscribe(topics);
            _logger.LogInformation(
                "Subscribed to topic(s): [{Topics}] with GroupId '{GroupId}'",
                string.Join(", ", topics),
                _options.GroupId);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<TKey, TValue>? consumeResult = null;
                try
                {
                    // Blocking consume with cancellation token
                    consumeResult = consumer.Consume(stoppingToken);

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        continue;
                    }

                    _logger.LogDebug(
                        "Received message from topic '{Topic}', partition {Partition}, offset {Offset}",
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);

                    // Execute handler in dedicated DI scope
                    using (var scope = _serviceProvider.CreateScope())
                    {
                        var handler = scope.ServiceProvider.GetRequiredService<IKafkaMessageHandler<TKey, TValue>>();
                        try
                        {
                            await handler.HandleAsync(consumeResult, stoppingToken);

                            // If auto-commit is disabled, commit manually upon successful processing
                            if (!_options.EnableAutoCommit)
                            {
                                consumer.Commit(consumeResult);
                            }
                        }
                        catch (Exception handlerEx)
                        {
                            _logger.LogError(
                                handlerEx,
                                "Error while handling Kafka message at offset {Offset} in partition {Partition}",
                                consumeResult.Offset.Value,
                                consumeResult.Partition.Value);

                            await handler.OnErrorAsync(handlerEx, consumeResult, stoppingToken);

                            // Re-throw or skip depending on requirements; here we do not commit so message can be reprocessed
                            throw;
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Consumer cancellation requested. Stopping loop.");
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(
                        ex,
                        "Kafka consume error: {Reason} (Code: {Code})",
                        ex.Error.Reason,
                        ex.Error.Code);

                    // Brief backoff on transient network/broker errors
                    await Task.Delay(1000, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in consumer loop");
                    await Task.Delay(1000, stoppingToken);
                }
            }
        }
        finally
        {
            try
            {
                _logger.LogInformation("Closing Kafka consumer connection...");
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Exception during consumer close.");
            }
        }
    }
}

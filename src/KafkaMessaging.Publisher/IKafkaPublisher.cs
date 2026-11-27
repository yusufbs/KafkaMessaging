using Confluent.Kafka;

namespace KafkaMessaging.Publisher;

/// <summary>
/// Defines Kafka publisher operations for strongly-typed messages.
/// </summary>
/// <typeparam name="TKey">Message key type</typeparam>
/// <typeparam name="TValue">Message value type</typeparam>
public interface IKafkaPublisher<TKey, TValue> : IDisposable
{
    /// <summary>
    /// Publishes a message with a specific key to a target topic.
    /// </summary>
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        TKey key,
        TValue value,
        Headers? headers = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to the configured default topic with a key.
    /// </summary>
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        TKey key,
        TValue value,
        Headers? headers = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message without an explicit key to a target topic.
    /// </summary>
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        TValue value,
        Headers? headers = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any pending messages in producer queue up to specified timeout.
    /// </summary>
    void Flush(TimeSpan timeout);
}

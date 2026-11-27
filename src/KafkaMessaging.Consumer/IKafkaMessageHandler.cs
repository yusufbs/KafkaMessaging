using Confluent.Kafka;

namespace KafkaMessaging.Consumer;

/// <summary>
/// Handler interface to process consumed Kafka messages.
/// Handlers are resolved from DI within a scoped lifetime for every message.
/// </summary>
/// <typeparam name="TKey">Message key type</typeparam>
/// <typeparam name="TValue">Message value type</typeparam>
public interface IKafkaMessageHandler<TKey, TValue>
{
    /// <summary>
    /// Processes a successfully consumed Kafka message.
    /// </summary>
    /// <param name="message">The consumed message result from Kafka</param>
    /// <param name="cancellationToken">Cancellation token for shutdown</param>
    Task HandleAsync(ConsumeResult<TKey, TValue> message, CancellationToken cancellationToken);

    /// <summary>
    /// Hook executed when an exception occurs while handling a message.
    /// Can be used for custom dead-letter queueing or alerting.
    /// </summary>
    Task OnErrorAsync(Exception exception, ConsumeResult<TKey, TValue>? message, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

using Confluent.Kafka;
using KafkaMessaging.Consumer.Sample.Models;
using Microsoft.Extensions.Logging;

namespace KafkaMessaging.Consumer.Sample.Handlers;

/// <summary>
/// Domain handler for OrderCreatedEvent messages.
/// Instantiated within a scoped lifetime for every incoming message.
/// </summary>
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
        var key = message.Message.Key;

        _logger.LogInformation(
            "===> [HANDLED ORDER] Key: {Key} | Partition: {Partition} | Offset: {Offset}",
            key,
            message.Partition.Value,
            message.Offset.Value);

        _logger.LogInformation("     Order Details: {Order}", order);

        // Extract sample headers if present
        if (message.Message.Headers != null)
        {
            foreach (var header in message.Message.Headers)
            {
                var headerVal = System.Text.Encoding.UTF8.GetString(header.GetValueBytes());
                _logger.LogDebug("     Header [{Key}] = {Value}", header.Key, headerVal);
            }
        }

        return Task.CompletedTask;
    }

    public Task OnErrorAsync(Exception exception, ConsumeResult<string, OrderCreatedEvent>? message, CancellationToken cancellationToken)
    {
        _logger.LogError(
            exception,
            "Error handling order with Key: {Key} on Partition: {Partition}, Offset: {Offset}",
            message?.Message?.Key,
            message?.Partition.Value,
            message?.Offset.Value);

        return Task.CompletedTask;
    }
}

namespace KafkaMessaging.Consumer.Sample.Models;

/// <summary>
/// Sample domain event payload consumed from Kafka.
/// </summary>
public class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public List<string> Items { get; set; } = new();
    public DateTime CreatedAtUtc { get; set; }

    public override string ToString() =>
        $"[Order #{OrderId}] Customer: {CustomerName}, Amount: {TotalAmount:C}, Items: {string.Join(", ", Items)} at {CreatedAtUtc:u}";
}

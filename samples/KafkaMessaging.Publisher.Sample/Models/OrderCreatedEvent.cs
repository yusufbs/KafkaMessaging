namespace KafkaMessaging.Publisher.Sample.Models;

/// <summary>
/// Sample domain event payload sent across Kafka.
/// </summary>
public class OrderCreatedEvent
{
    public Guid OrderId { get; set; } = Guid.NewGuid();
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public List<string> Items { get; set; } = new();
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

    public override string ToString() =>
        $"[Order #{OrderId}] Customer: {CustomerName}, Amount: {TotalAmount:C}, Items: {string.Join(", ", Items)} at {CreatedAtUtc:u}";
}

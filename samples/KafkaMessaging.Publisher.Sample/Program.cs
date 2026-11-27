using Confluent.Kafka;
using KafkaMessaging.Publisher;
using KafkaMessaging.Publisher.Extensions;
using KafkaMessaging.Publisher.Sample.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

// Register Kafka Publisher from appsettings.json section "Kafka:Publisher"
builder.Services.AddKafkaPublisher<string, OrderCreatedEvent>(builder.Configuration);

var host = builder.Build();

var logger = host.Services.GetRequiredService<ILogger<Program>>();
var publisher = host.Services.GetRequiredService<IKafkaPublisher<string, OrderCreatedEvent>>();

Console.WriteLine("==================================================");
Console.WriteLine("    Kafka Publisher Sample Application (.NET)      ");
Console.WriteLine("==================================================");
Console.WriteLine("Publishing 5 sample orders to Kafka...\n");

var random = new Random();
var itemsPool = new[] { "Mechanical Keyboard", "UltraWide Monitor", "Ergonomic Chair", "USB-C Hub", "Noise Cancelling Headphones" };
var customers = new[] { "Alice Johnson", "Bob Smith", "Charlie Davis", "Diana Prince", "Evan Wright" };

for (int i = 1; i <= 5; i++)
{
    var order = new OrderCreatedEvent
    {
        OrderId = Guid.NewGuid(),
        CustomerName = customers[random.Next(customers.Length)],
        TotalAmount = Math.Round((decimal)(random.NextDouble() * 500 + 20), 2),
        Items = itemsPool.OrderBy(_ => random.Next()).Take(random.Next(1, 4)).ToList(),
        CreatedAtUtc = DateTime.UtcNow
    };

    var headers = new Headers
    {
        { "source-system", System.Text.Encoding.UTF8.GetBytes("OrderCheckoutService") },
        { "correlation-id", System.Text.Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()) }
    };

    try
    {
        // Produce message using the order ID as partition key and the default configured topic
        var result = await publisher.ProduceAsync(
            key: order.OrderId.ToString(),
            value: order,
            headers: headers);

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[Sent {i}/5] Partition: {result.Partition.Value}, Offset: {result.Offset.Value}");
        Console.ResetColor();
        Console.WriteLine($"  Payload: {order}\n");
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"[Error sending {i}/5]: {ex.Message}");
        Console.ResetColor();
    }

    await Task.Delay(1000);
}

publisher.Flush(TimeSpan.FromSeconds(5));
Console.WriteLine("All test orders sent successfully.");

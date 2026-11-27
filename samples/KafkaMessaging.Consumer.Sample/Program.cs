using KafkaMessaging.Consumer.Extensions;
using KafkaMessaging.Consumer.Sample.Handlers;
using KafkaMessaging.Consumer.Sample.Models;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

// Register Kafka Consumer background service and scoped message handler
builder.Services.AddKafkaConsumer<string, OrderCreatedEvent, OrderCreatedEventHandler>(builder.Configuration);

Console.WriteLine("==================================================");
Console.WriteLine("     Kafka Consumer Sample Worker (.NET)          ");
Console.WriteLine("==================================================");
Console.WriteLine("Starting consumer service. Waiting for messages...\n");

var app = builder.Build();
await app.RunAsync();

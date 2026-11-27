using KafkaMessaging.Consumer.BackgroundServices;
using KafkaMessaging.Consumer.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KafkaMessaging.Consumer.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers Kafka consumer background service and handler with Action configuration.
    /// </summary>
    public static IServiceCollection AddKafkaConsumer<TKey, TValue, THandler>(
        this IServiceCollection services,
        Action<KafkaConsumerOptions> configure)
        where THandler : class, IKafkaMessageHandler<TKey, TValue>
    {
        services.Configure(configure);
        services.AddScoped<IKafkaMessageHandler<TKey, TValue>, THandler>();
        services.AddHostedService<KafkaConsumerBackgroundService<TKey, TValue>>();
        return services;
    }

    /// <summary>
    /// Registers Kafka consumer background service and handler binding from IConfiguration.
    /// </summary>
    public static IServiceCollection AddKafkaConsumer<TKey, TValue, THandler>(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaConsumerOptions.SectionName)
        where THandler : class, IKafkaMessageHandler<TKey, TValue>
    {
        var section = configuration.GetSection(sectionName);
        services.Configure<KafkaConsumerOptions>(section);
        services.AddScoped<IKafkaMessageHandler<TKey, TValue>, THandler>();
        services.AddHostedService<KafkaConsumerBackgroundService<TKey, TValue>>();
        return services;
    }

    /// <summary>
    /// Convenience registration for string-keyed consumer IKafkaMessageHandler&lt;string, TValue&gt;.
    /// </summary>
    public static IServiceCollection AddKafkaConsumer<TValue, THandler>(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaConsumerOptions.SectionName)
        where THandler : class, IKafkaMessageHandler<string, TValue>
    {
        return services.AddKafkaConsumer<string, TValue, THandler>(configuration, sectionName);
    }
}

using Confluent.Kafka;
using KafkaMessaging.Publisher.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KafkaMessaging.Publisher.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers IKafkaPublisher&lt;TKey, TValue&gt; with delegate configuration.
    /// </summary>
    public static IServiceCollection AddKafkaPublisher<TKey, TValue>(
        this IServiceCollection services,
        Action<KafkaPublisherOptions> configure)
    {
        services.Configure(configure);
        services.TryAddSingleton(typeof(IKafkaPublisher<,>), typeof(KafkaPublisher<,>));
        services.TryAddSingleton<IKafkaPublisher<TKey, TValue>, KafkaPublisher<TKey, TValue>>();
        return services;
    }

    /// <summary>
    /// Registers IKafkaPublisher&lt;TKey, TValue&gt; binding from IConfiguration.
    /// </summary>
    public static IServiceCollection AddKafkaPublisher<TKey, TValue>(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaPublisherOptions.SectionName)
    {
        var section = configuration.GetSection(sectionName);
        services.Configure<KafkaPublisherOptions>(section);
        services.TryAddSingleton(typeof(IKafkaPublisher<,>), typeof(KafkaPublisher<,>));
        services.TryAddSingleton<IKafkaPublisher<TKey, TValue>, KafkaPublisher<TKey, TValue>>();
        return services;
    }

    /// <summary>
    /// Convenience registration for string-keyed publisher IKafkaPublisher&lt;string, TValue&gt;.
    /// </summary>
    public static IServiceCollection AddKafkaPublisher<TValue>(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = KafkaPublisherOptions.SectionName)
    {
        return services.AddKafkaPublisher<string, TValue>(configuration, sectionName);
    }
}

using System.Text.Json;
using Confluent.Kafka;

namespace KafkaMessaging.Publisher.Serialization;

/// <summary>
/// Generic JSON serializer using System.Text.Json compatible with Confluent.Kafka.
/// </summary>
/// <typeparam name="T">Payload type to serialize</typeparam>
public class DefaultJsonSerializer<T> : ISerializer<T>
{
    private readonly JsonSerializerOptions _options;

    public DefaultJsonSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data is null)
            return Array.Empty<byte>();

        return JsonSerializer.SerializeToUtf8Bytes(data, _options);
    }
}

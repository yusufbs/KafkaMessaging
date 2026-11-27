using System.Text.Json;
using Confluent.Kafka;

namespace KafkaMessaging.Consumer.Serialization;

/// <summary>
/// Generic JSON deserializer using System.Text.Json compatible with Confluent.Kafka.
/// </summary>
/// <typeparam name="T">Payload type to deserialize</typeparam>
public class DefaultJsonDeserializer<T> : IDeserializer<T>
{
    private readonly JsonSerializerOptions _options;

    public DefaultJsonDeserializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
    }

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
            return default!;

        return JsonSerializer.Deserialize<T>(data, _options)!;
    }
}

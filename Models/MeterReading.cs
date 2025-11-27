namespace MqttTestClient.Models;

public class MeterReading(int clientId) : Entity(clientId)
{
    private readonly DateTime? _readingOn;

    public long? MeterId { get; init; }
    public decimal? Reading { get; set; }

    public DateTime? ReadingOn
    {
        get => _readingOn;
        init => SetDateTime(ref _readingOn, value);
    }
}
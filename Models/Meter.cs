namespace MqttTestClient.Models;

public class Meter(int clientId) : Entity(clientId)
{
    public required string? Code { get; init; }
    public required string? Description { get; set; }
    public bool IsPaused { get; set; }
    public decimal? Adr { get; init; }
}
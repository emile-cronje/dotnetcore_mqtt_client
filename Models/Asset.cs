namespace MqttTestClient.Models;

public class Asset(int clientId) : Entity(clientId)
{
    public required string? Code { get; init; }
    public required string? Description { get; set; }
    public bool IsMsi { get; set; }
}
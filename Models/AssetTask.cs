namespace MqttTestClient.Models;

public class AssetTask(int clientId) : Entity(clientId)
{
    public long? AssetId { get; init; }
    public required string? Code { get; init; }
    public required string? Description { get; set; }
    public bool IsRfs { get; set; }
}
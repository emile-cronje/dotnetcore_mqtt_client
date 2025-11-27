namespace MqttTestClient.Models;

public class ToDoItem(int clientId) : Entity(clientId)
{
    public required string? Name { get; init; }
    public required string? Description { get; set; }
    public bool IsComplete { get; set; }
}
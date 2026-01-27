namespace MqttTestClient.Models;

public class Entity(int clientId)
{
    public long? Id { get; init; }
    public int Version { get; init; }
    public int ClientId { get; } = clientId;
    public string MessageId { get; set; }    
    public Guid Guid { get; set; }

    protected void SetDateTime(ref DateTime? oldValue, DateTime? newValue)
    {
        if (newValue.HasValue)
            newValue = DateTime.SpecifyKind(newValue.Value, DateTimeKind.Local);

        SetProperty(ref oldValue, newValue);
    }

    private void SetProperty<T>(ref T oldValue, T newValue)
    {
        SetPropertyValue(ref oldValue, newValue);
    }

    private void SetPropertyValue<T>(ref T oldValue, T newValue)
    {
        oldValue = newValue;
    }
}
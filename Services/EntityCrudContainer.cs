using System.Collections.Concurrent;

namespace MqttTestClient.Services;

public class EntityCrudContainer
{
    public readonly ConcurrentQueue<CrudCommand> CreateEntityQueue = new();
    public readonly Queue<CrudCommand> DeleteEntityQueue = new();
    public readonly bool DoInsert = true;
    public readonly Queue<CrudCommand> UpdateEntityQueue = new();
    public bool DoDelete = false;
    public bool DoUpdate = false;
}
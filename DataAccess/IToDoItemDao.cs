using MqttTestClient.Models;

namespace MqttTestClient.DataAccess;

public interface IToDoItemDao : IPgDao
{
    void Initialise();
    Task<ToDoItem?> GetItem(long itemId);
    Task BatchInsert(List<ToDoItem> items);
    Task BatchDelete(IEnumerable<long> ids);    
    Task BatchUpdate(List<ToDoItem> items, int batchSize);
}
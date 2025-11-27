using System.Collections.Concurrent;

namespace MqttTestClient.Services.Queues;

public class ClientQueueManager<T>
{
    private readonly Dictionary<string, ConcurrentQueue<T>> _queues;
    private readonly Lock _syncRoot = new();

    public ClientQueueManager()
    {
        _queues = new Dictionary<string, ConcurrentQueue<T>>();
    }

    public void Enqueue(string clientId, T item)
    {
        lock (_syncRoot)
        {
            if (!_queues.ContainsKey(clientId))
                _queues[clientId] = new ConcurrentQueue<T>();

            _queues[clientId].Enqueue(item);
        }
    }

    public IEnumerable<T> FilterQueue(string clientId, Func<T, bool> predicate)
    {
        lock (_syncRoot)
        {
            return _queues.TryGetValue(clientId, out var queue) ? queue.Where(predicate) : [];
        }
    }
}
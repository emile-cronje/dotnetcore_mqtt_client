namespace MqttTestClient.Services;

public class ListManager<T>
{
    private readonly Dictionary<string, List<T>?> _lists;
    private readonly Lock _syncRoot = new();

    public ListManager()
    {
        _lists = new Dictionary<string, List<T>?>();
    }

    public void AddItem(string clientId, T item)
    {
        lock (_syncRoot)
        {
            if (!_lists.ContainsKey(clientId)) _lists[clientId] = new List<T>();

            _lists[clientId]?.Add(item);
        }
    }

    private List<T> GetAllLists()
    {
        lock (_syncRoot)
        {
            var result = new List<T>();
            
            foreach (var item in _lists)
            {
                if (item.Value != null)
                    result.AddRange(item.Value);    
            }

            return result;
        }
    }
    
    public IEnumerable<T> FilterList(string clientId, Func<T, bool> predicate)
    {
        lock (_syncRoot)
        {
            if (_lists.TryGetValue(clientId, out var list))
                if (list != null)
                    return list.Where(predicate).ToList();

            return [];
        }
    }

    public IEnumerable<T> FilterAllLists(Func<T, bool> predicate)
    {
        lock (_syncRoot)
        {
            return GetAllLists().Where(predicate).ToList();
        }
    }
}
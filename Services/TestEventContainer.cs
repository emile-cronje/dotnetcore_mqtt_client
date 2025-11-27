using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using MqttTestClient.Models;
using MqttTestClient.Services.Queues;

namespace MqttTestClient.Services;

public class TestEventContainer
{
    private readonly ConcurrentDictionary<EntityType, HashSet<long>> _entities = new();
    private readonly Lock _entityLock = new();
    private readonly Lock _lock = new();
    private readonly Lock _syncRoot = new();
    public readonly ManualResetEvent AllInsertsCompletedEvent = new(false);
    public readonly ConcurrentDictionary<Guid, long> AssetIdsForTasks = new();
    public readonly ClientQueueManager<Entity> CreateQueueManager = new();
    public readonly ListManager<Entity> DeletedChildEntityList = new();
    public readonly ListManager<Entity> IgnoredChildEntityList = new();    
    public readonly ListManager<Entity> DeletedParentEntityList = new();
    public readonly ListManager<Entity> IgnoredParentEntityList = new();    
    public readonly Dictionary<EntityType, EntityContainer?> EntityContainers = new();
    public readonly ConcurrentDictionary<Guid, long> MeterIdsForReadings = new();
    public readonly Dictionary<long, SequentialSequenceGenerator> MeterReadingsSequencer = new();
    public readonly ManualResetEvent StartAssetTasksEvent = new(false);
    public readonly ManualResetEvent StartMeterReadingsEvent = new(false);
    public readonly ManualResetEvent StartTestEvent = new(false);
    public readonly ManualResetEvent StartCompareMonitorEvent = new(false);    
    public readonly Stopwatch Stopwatch = new();
    public readonly ClientQueueManager<Entity> UpdateQueueManager = new();
    private bool _compareTestStarted;
    private int _totalIgnoredInserts;

    public bool CompareTestStarted
    {
        get
        {
            lock (_lock)
            {
                return _compareTestStarted;
            }
        }
        set
        {
            lock (_lock)
            {
                _compareTestStarted = value;
            }
        }
    }

    public List<T> GetAllDeletedChildEntityLists<T>() where T : Entity
    {
        lock (_syncRoot)
        {
            var items = DeletedChildEntityList.FilterAllLists(item => item is T);
            return items.Cast<T>().ToList();
        }
    }

    public List<T> GetAllIgnoredChildEntityLists<T>() where T : Entity
    {
        lock (_syncRoot)
        {
            var items = IgnoredChildEntityList.FilterAllLists(item => item is T);
            return items.Cast<T>().ToList();
        }
    }
    
    public List<T> GetAllDeletedParentEntityLists<T>() where T : Entity
    {
        lock (_syncRoot)
        {
            var items = DeletedParentEntityList.FilterAllLists(item => item is T);
            return items.Cast<T>().ToList();
        }
    }

    public List<T> GetAllIgnoredParentEntityLists<T>() where T : Entity
    {
        lock (_syncRoot)
        {
            var items = IgnoredParentEntityList.FilterAllLists(item => item is T);
            return items.Cast<T>().ToList();
        }
    }
    
    public Queue<T> GetCreateQueue<T>(string clientId) where T : Entity
    {
        lock (_syncRoot)
        {
            var result = new Queue<T>();
            var items = CreateQueueManager.FilterQueue(clientId, item => item is T);

            foreach (var item in items) result.Enqueue((T)item);

            return result;
        }
    }

    public int GetIgnoredInsertsCount()
    {
        lock (_syncRoot)
        {
            return _totalIgnoredInserts;
        }
    }

    public void AddToTotalIgnoredInsertsCount(int count = 1)
    {
        lock (_syncRoot)
        {
            _totalIgnoredInserts += count;
        }
    }

    public void AddEntity(EntityType entityType, long entityId)
    {
        lock (_entityLock)
        {
            if (_entities.TryGetValue(entityType, out var entityIdList))
                entityIdList.Add(entityId);
            else
                _entities.TryAdd(entityType, [entityId]);
        }
    }

    public long? GetEntityId(EntityType entityType)
    {
        lock (_entityLock)
        {
            if (_entities.TryGetValue(entityType, out var entityIdList))
            {
                if (entityIdList.Count == 0)
                    return null;

                var random = new Random();
                var index = random.Next(0, entityIdList.Count);
                return entityIdList.ElementAt(index);
            }

            return null;
        }
    }

    public void RemoveEntityId(EntityType entityType, long entityId)
    {
        lock (_entityLock)
        {
            if (_entities.TryGetValue(entityType, out var entityIdList))
                entityIdList.Remove(entityId);
        }
    }


    public class SequentialSequenceGenerator(int start, int count) : IEnumerable<int>
    {
        private int _current = start;

        public IEnumerator<int> GetEnumerator()
        {
            for (var i = 0; i < count; i++) yield return _current++;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
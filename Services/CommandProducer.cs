namespace MqttTestClient.Services;

public class CommandProducer
{
    private readonly int _entityCount;
    private readonly EntityCrudContainer _entityCrudContainer;
    private readonly bool _doAssets;
    private readonly bool _doAssetTasks;
    private readonly bool _doItems;
    private readonly bool _doMeterReadings;
    private readonly bool _doMeters;
    private readonly int _meterReadingsPerMeterCount;
    private readonly int _tasksPerAssetCount;
    private readonly int _entityUpdateCount;
    private readonly int _entityDeletePerEntityTypeCount;    

    public CommandProducer(int entityCount, EntityCrudContainer entityCrudContainer,
        bool doItems, bool doAssets, bool doAssetTasks, bool doMeters, bool doMeterReadings,
        int meterReadingsPerMeterCount, int tasksPerAssetCount, int entityUpdateCount,
        int entityDeletePerEntityTypeCount)
    {
        _entityCount = entityCount;
        _entityCrudContainer = entityCrudContainer;
        _doItems = doItems;
        _doAssets = doAssets;
        _doAssetTasks = doAssetTasks;
        _doMeters = doMeters;
        _doMeterReadings = doMeterReadings;
        _meterReadingsPerMeterCount = meterReadingsPerMeterCount;
        _tasksPerAssetCount = tasksPerAssetCount;
        _entityUpdateCount = entityUpdateCount;
        _entityDeletePerEntityTypeCount = entityDeletePerEntityTypeCount;
    }

    public async Task Run(Guid mqttSessionId, List<string> clientIds)
    {
        var addCount = 0;
        var updateCount = 0;        
        var deleteCount = 0;        
        var factor = 0;

        if (_doItems)
            factor += 1;

        if (_doAssets)
            factor += 1;

        if (_doAssetTasks)
            factor += 1 * _tasksPerAssetCount;

        if (_doMeters)
            factor += 1;

        if (_doMeterReadings)
            factor += 1 * _meterReadingsPerMeterCount;

        var totalEntityCount = _entityCount * clientIds.Count * factor;
        var clientIdArray = clientIds.ToArray();

        ShuffleStrings(clientIdArray);

        var entityTypes = GetRelevantEntityTypes();

        await Task.Run(() =>
        {
            while (addCount < totalEntityCount)
            {
                foreach (var clientId in clientIdArray)
                foreach (var entityType in entityTypes)
                {
                    addCount += ProcessCreateCommand(mqttSessionId, clientId, CrudOperation.Create, entityType);
                    
                    if (_entityCrudContainer.DoUpdate)
                        updateCount += ProcessUpdateCommand(mqttSessionId, clientId, entityType);                    
                }
            }

            if (_entityCrudContainer.DoDelete)
            {
                foreach (var clientId in clientIdArray)
                    foreach (var entityType in entityTypes)
                    {
                        deleteCount += ProcessDeleteCommand(mqttSessionId, clientId, entityType);                    
                    }
            }
            
            Console.WriteLine("Add count: " + addCount);
            Console.WriteLine("Update count: " + updateCount);            
            Console.WriteLine("Delete count: " + deleteCount);            
        });
    }

    private int ProcessCreateCommand(Guid mqttSessionId, string clientId, CrudOperation operation, EntityType entityType)
    {
        var count = 0;

        // Parents with children
        if (operation == CrudOperation.Create && entityType == EntityType.Asset)
        {
            var assetGuid  = Guid.NewGuid();
            var assetCrudCommand = new CrudCommand(mqttSessionId, clientId, entityType, operation, true, assetGuid);
            _entityCrudContainer.CreateEntityQueue.Enqueue(assetCrudCommand);
            int taskCount = 0;
            count++;            
            
            while (_doAssetTasks && (taskCount < _tasksPerAssetCount))
            {
                taskCount++;
                var taskGuid  = Guid.NewGuid();                
                var taskCrudCommand = new CrudCommand(mqttSessionId, clientId, EntityType.AssetTask, CrudOperation.Create,
                    false, taskGuid, assetGuid);
                _entityCrudContainer.CreateEntityQueue.Enqueue(taskCrudCommand);
            }
            
            count += taskCount;            
        }
        else if (operation == CrudOperation.Create && entityType == EntityType.Meter)
        {
            count++;
            var meterGuid  = Guid.NewGuid();
            var meterCrudCommand = new CrudCommand(mqttSessionId, clientId, entityType, operation, true, meterGuid);
            _entityCrudContainer.CreateEntityQueue.Enqueue(meterCrudCommand);
            
            int readingCount = 0;
            
            while (_doMeterReadings && readingCount < _meterReadingsPerMeterCount)
            {
                readingCount++;
                var readingGuid  = Guid.NewGuid();                
                var readingCrudCommand = new CrudCommand(mqttSessionId, clientId, EntityType.MeterReading, CrudOperation.Create, false, readingGuid, meterGuid);
                _entityCrudContainer.CreateEntityQueue.Enqueue(readingCrudCommand);
            }
            
            count += readingCount;            
        }
        else if (operation == CrudOperation.Create && entityType == EntityType.ToDoItem)
        {
            count++;
            var toDoGuid  = Guid.NewGuid();
            var toDoCrudCommand = new CrudCommand(mqttSessionId, clientId, entityType, operation, true, toDoGuid);
            _entityCrudContainer.CreateEntityQueue.Enqueue(toDoCrudCommand);
        }

        return count;
    }

    private int ProcessUpdateCommand(Guid mqttSessionId, string clientId, EntityType entityType)
    {
        var count = 0;

        {
            while (count < _entityUpdateCount)
            {
                var guid = Guid.NewGuid();
                var updateCommand = new CrudCommand(mqttSessionId, clientId, entityType, CrudOperation.Update, IsParentEntityType(entityType), guid);                
                _entityCrudContainer.UpdateEntityQueue.Enqueue(updateCommand);
                count++;                
            }
        }

        return count;
    }

    private bool IsParentEntityType(EntityType entityType)
    {
        return entityType is EntityType.Asset or EntityType.Meter;    
    }
    
    private int ProcessDeleteCommand(Guid mqttSessionId, string clientId, EntityType entityType)
    {
        var count = 0;

        {
            while (count < _entityDeletePerEntityTypeCount)
            {
                var guid = Guid.NewGuid();                
                var deleteCommand = new CrudCommand(mqttSessionId, clientId, entityType, CrudOperation.Delete, IsParentEntityType(entityType), guid);                
                _entityCrudContainer.DeleteEntityQueue.Enqueue(deleteCommand);
                count++;                
            }
        }

        return count;
    }
    
    private List<EntityType> GetRelevantEntityTypes()
    {
        var result = new List<EntityType>();

        var entityTypes = (EntityType[])Enum.GetValues(typeof(EntityType));

        foreach (var entityType in entityTypes)
            if (_doItems && entityType == EntityType.ToDoItem)
                result.Add(entityType);
            else if (_doAssets && entityType == EntityType.Asset)
                result.Add(entityType);
            else if (_doMeters && entityType == EntityType.Meter)
                result.Add(entityType);
            else if (_doAssetTasks && entityType == EntityType.AssetTask)
                result.Add(entityType);
            else if (_doMeterReadings && entityType == EntityType.MeterReading)
                result.Add(entityType);

        return result;
    }

    private static void ShuffleStrings(string[] array)
    {
        var n = array.Length;
        var random = new Random();

        for (var i = n - 1; i > 0; i--)
        {
            var j = random.Next(0, i + 1);
            (array[i], array[j]) = (array[j], array[i]);
        }
    }
}

public class CrudCommand
{
    public CrudCommand(Guid mqttSessionId, string clientId, EntityType entityType, CrudOperation crudOperation, bool isParent,
        Guid guid, Guid? parentGuid = null)
    {
        ClientId = clientId;
        EntityType = entityType;
        CrudOperation = crudOperation;
        IsParent = isParent;
        Guid = guid;
        ParentGuid = parentGuid;
        MqttSessionId = mqttSessionId;
    }

    public string ClientId { get; private set; }
    public EntityType EntityType { get; }
    public CrudOperation CrudOperation { get; }
    public bool IsParent { get; }
    public Guid Guid { get; set; }
    public Guid? ParentGuid { get; set; }    
    public Guid MqttSessionId { get; set; }        
}

public enum CrudOperation
{
    Create,
    Update,
    Delete
}

public enum EntityType
{
    Asset,
    AssetTask,
    Meter,
    MeterReading,
    ToDoItem
}
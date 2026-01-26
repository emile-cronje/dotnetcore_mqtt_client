using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using MqttTestClient.Controllers;
using MqttTestClient.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace MqttTestClient.Services.Web;

public interface IWebClientService;

public class WebClientService : BackgroundService, IWebClientService
{
    private readonly AssetClient _assetClient;
    private readonly AssetTaskClient _assetTaskClient;
    private readonly List<string> _clientIds;
    private readonly CommandConsumer _commandConsumer;
    private readonly CommandProducer _commandProducer;
    private readonly bool _doAssets;
    private readonly bool _doAssetTasks;
    private readonly bool _doItems;
    private readonly bool _doMeterReadings;
    private readonly bool _doMeters;
    private readonly MeterClient _meterClient;
    private readonly MeterReadingClient _meterReadingClient;
    private readonly TestEventContainer _testEventContainer;
    private readonly ToDoItemClient _toDoItemClient;
    private readonly ToDoItemController _toDoItemController;
    private readonly AssetController _assetController;    
    private readonly MeterController _meterController;
    private readonly AssetTaskController _assetTaskController;
    private readonly MeterReadingController _meterReadingController;    
    private readonly Guid _mqttSessionId;

    public WebClientService(List<string> clientIds,
        int totalEntityCount,
        int entityUpdateCount,
        int deletePerEntityTypeCount,
        int meterReadingsPerMeterCount,
        int tasksPerAssetCount,
        TestEventContainer testEventContainer,
        bool doItems,
        bool doAssets,
        bool doAssetTasks,
        bool doMeters,
        bool doMeterReadings,
        ToDoItemClient toDoClient,
        AssetClient assetClient, AssetTaskClient assetTaskClient,
        MeterClient meterClient, MeterReadingClient meterReadingClient,
        EntityCrudContainer entityCrudContainer,
        ToDoItemController toDoItemController, AssetController assetController,
        MeterController meterController,
        AssetTaskController assetTaskController,
        MeterReadingController meterReadingController,
        Guid mqttSessionId)
    {
        _clientIds = clientIds;
        _doItems = doItems;
        _doAssets = doAssets;
        _doMeters = doMeters;
        _doMeterReadings = doMeterReadings;
        _doAssetTasks = doAssetTasks;
        _toDoItemClient = toDoClient;
        _assetClient = assetClient;
        _assetTaskClient = assetTaskClient;
        _testEventContainer = testEventContainer;
        _meterClient = meterClient;
        _meterReadingClient = meterReadingClient;
        _toDoItemController = toDoItemController;
        _assetController = assetController;
        _meterController = meterController;
        _assetTaskController = assetTaskController;
        _meterReadingController = meterReadingController;
        _commandProducer = new CommandProducer(totalEntityCount, entityCrudContainer,
            doItems, doAssets, doAssetTasks, doMeters, doMeterReadings, meterReadingsPerMeterCount, tasksPerAssetCount,
            entityUpdateCount, deletePerEntityTypeCount);
        _commandConsumer = new CommandConsumer(this, entityCrudContainer, _testEventContainer, toDoItemController,
            assetController, meterController, assetTaskController, meterReadingController);
        _mqttSessionId = mqttSessionId;            
    }

    protected override async Task ExecuteAsync(CancellationToken stopToken)
    {
        await Run();
    }

    private async Task Run()
    {
        _testEventContainer.Stopwatch.Start();

        if (_doItems)
            await _toDoItemClient.DeleteAllAsync();

        if (_doAssetTasks)
            await _assetTaskClient.DeleteAllAsync();

        if (_doAssets)
        {
            if (_doAssetTasks)            
                await _assetTaskClient.DeleteAllAsync();
            
            await _assetClient.DeleteAllAsync();
        }

        if (_doMeterReadings)
            await _meterReadingClient.DeleteAllAsync();

        if (_doMeters)
        {
            if (_doMeterReadings)            
                await _meterReadingClient.DeleteAllAsync();
            
            await _meterClient.DeleteAllAsync();
        }

        List<Task> tasks = [];

        var producerTask = _commandProducer.Run(_mqttSessionId, _clientIds);
        await Task.WhenAll(new List<Task> { producerTask });

        var consumerTask = _commandConsumer.Run();
        tasks.Add(consumerTask);

        await Task.WhenAll(tasks);
    }

    public async Task ProcessItemPostAsync(Guid mqttSessionId, string clientId)
    {
        if (!_doItems)
            return;

        var itemNamePrefix = "Item_Name_";
        var itemDescriptionPrefix = "Item_Description_";

        string messageId = _toDoItemController.GetNextEntityMessageId;
        var itemName = clientId + '_' + itemNamePrefix + messageId;
        var itemDescription = itemDescriptionPrefix + messageId;
        var isComplete = false;

        var insertData = _toDoItemClient.BuildPostData(messageId.ToString(), clientId,
            itemName, itemDescription, isComplete);

        _toDoItemController.EntityContainer?.AddInsertMessageId(messageId);        
        await _toDoItemClient.PostAsync(mqttSessionId, insertData);
    }

    public async Task<string> ProcessItemPutAsync(Guid mqttSessionId, ToDoItem toDoItem)
    {
        toDoItem.IsComplete = true;

        var namingStrategy = new CamelCaseNamingStrategy();
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = namingStrategy },
            Formatting = Formatting.Indented
        };

        string messageId = _toDoItemController.GetNextEntityMessageId;
        toDoItem.MessageId = messageId;        
        toDoItem.Description += "_Updated_" + messageId;
        var toDoItemJson = JsonConvert.SerializeObject(toDoItem, jsonSerializerSettings);

        if (toDoItem.Id != null)
        {
            _toDoItemController.EntityContainer?.AddUpdateMessageId(messageId);
            await _toDoItemClient.PutAsync(mqttSessionId, toDoItem.Id.Value, toDoItemJson);
        }
        
        return messageId;
    }

    public async Task<string> ProcessItemDeleteAsync(Guid mqttSessionId, long toDoItemId)
    {
        string messageId = _toDoItemController.GetNextEntityMessageId;
        _toDoItemController.EntityContainer?.AddDeleteMessageId(messageId);        
        await _toDoItemClient.DeleteAsync(toDoItemId, messageId, mqttSessionId);
        return messageId;
    }

    public async Task ProcessAssetPostAsync(Guid mqttSessionId, CrudCommand command)
    {
        if (!_doAssets)
            return;

        var assetCodePrefix = "Asset_Code_";
        var assetDescriptionPrefix = "Asset_Description_";

        string messageId = _assetController.GetNextEntityMessageId;
        var assetCode = command.ClientId + '_' + assetCodePrefix + messageId;
        var assetDescription = assetDescriptionPrefix + messageId;
        var isMsi = true;

        var insertData = _assetClient.BuildPostData(messageId.ToString(), command.ClientId,
            command.Guid.ToString(), assetCode, assetDescription, isMsi);

        _assetController.EntityContainer?.AddInsertMessageId(messageId);        
        await _assetClient.PostAsync(mqttSessionId, insertData);
    }

    public async Task ProcessMeterPostAsync(Guid mqttSessionId, CrudCommand command)
    {
        if (!_doMeters)
            return;

        var meterCodePrefix = "Meter_Code_";
        var meterDescriptionPrefix = "Meter_Description_";

        string messageId = _meterController.GetNextEntityMessageId;
        var meterCode = command.ClientId + '_' + meterCodePrefix + messageId;
        var meterDescription = meterDescriptionPrefix + messageId;
        var isPaused = false;

        var insertData = _meterClient.BuildPostData(messageId.ToString(), command.ClientId,
            command.Guid.ToString(), meterCode, meterDescription, isPaused);

        _testEventContainer.EntityContainers[EntityType.Meter]?.AddInsertMessageId(messageId);
        await _meterClient.PostAsync(mqttSessionId, insertData);
    }

    public async Task<string> ProcessMeterPutAsync(Guid mqttSessionId, Meter meter)
    {
        meter.IsPaused = !meter.IsPaused;

        var namingStrategy = new CamelCaseNamingStrategy();
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = namingStrategy },
            Formatting = Formatting.Indented
        };

        string messageId = _meterController.GetNextEntityMessageId;
        meter.MessageId = messageId;        
        meter.Description += "_Updated_" + messageId;
        var meterJson = JsonConvert.SerializeObject(meter, jsonSerializerSettings);

        if (meter.Id != null)
        {
            _meterController.EntityContainer?.AddUpdateMessageId(messageId);
            await _meterClient.PutAsync(mqttSessionId, meter.Id.Value, meterJson);
        }
        
        return messageId;
    }

    public async Task<string> ProcessMeterDeleteAsync(Guid mqttSessionId, long meterId)
    {
        string messageId = _meterController.GetNextEntityMessageId;
        _testEventContainer.EntityContainers[EntityType.Meter]?.AddDeleteMessageId(messageId);
        await _meterClient.DeleteAsync(meterId, messageId, mqttSessionId);
        return messageId;
    }

    public async Task<string> ProcessAssetPutAsync(Guid mqttSessionId, Asset asset)
    {
        asset.IsMsi = true;

        var namingStrategy = new CamelCaseNamingStrategy();

        var jsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = namingStrategy },
            Formatting = Formatting.Indented
        };

        string messageId = _assetController.GetNextEntityMessageId;
        asset.MessageId = messageId;        
        asset.Description += "_Updated_" + messageId;
        var assetJson = JsonConvert.SerializeObject(asset, jsonSerializerSettings);

        if (asset.Id != null)
        {
            _assetController.EntityContainer?.AddUpdateMessageId(messageId);
            await _assetClient.PutAsync(mqttSessionId, asset.Id.Value, assetJson);
        }
        
        return messageId;
    }

    public async Task<string> ProcessAssetDeleteAsync(Guid mqttSessionId, long assetId)
    {
        string messageId = _assetController.GetNextEntityMessageId;
        _assetController.EntityContainer?.AddDeleteMessageId(messageId);
        await _assetClient.DeleteAsync(assetId, messageId, mqttSessionId);
        return messageId;
    }

    public async Task ProcessAssetTaskPostAsync(Guid mqttSessionId, CrudCommand command)
    {
        if (!_doAssetTasks)
            return;

        _testEventContainer.StartAssetTasksEvent.WaitOne();

        var taskCodePrefix = "Task_Code_";
        var taskDescriptionPrefix = "Task_Description_";

        while (GetParentEntityId(_testEventContainer.AssetIdsForTasks, command.ParentGuid) == -1)
            await Task.Delay(1000);

        var assetId = GetParentEntityId(_testEventContainer.AssetIdsForTasks, command.ParentGuid);
        string messageId = _assetTaskController.GetNextEntityMessageId;
        var taskCode = command.ClientId + '_' + taskCodePrefix + messageId;
        var taskDescription = taskDescriptionPrefix + messageId;
        var isRfs = false;

        var insertData = _assetTaskClient.BuildPostData(messageId.ToString(), command.ClientId,
            assetId, taskCode, taskDescription, isRfs);

        _assetTaskController.EntityContainer?.AddInsertMessageId(messageId);
        await _assetTaskClient.PostAsync(mqttSessionId, insertData);
    }

    public async Task ProcessMeterReadingPostAsync(Guid mqttSessionId, CrudCommand command)
    {
        if (!_doMeterReadings)
            return;

        _testEventContainer.StartMeterReadingsEvent.WaitOne();

        while (GetParentEntityId(_testEventContainer.MeterIdsForReadings, command.ParentGuid) == -1)
            await Task.Delay(1000);

        var meterId = GetParentEntityId(_testEventContainer.MeterIdsForReadings, command.ParentGuid);
        string messageId = _meterReadingController.GetNextEntityMessageId;
        var random = new Random();
        var reading = (decimal)(1.0 + random.NextDouble() * 12.0);

        if (!_testEventContainer.MeterReadingsSequencer.TryGetValue(meterId, out var sequencer))
            return;

        using var enumerator = sequencer.GetEnumerator();

        if (!enumerator.MoveNext())
            return;

        var readingOn = DateTime.UtcNow.AddDays(enumerator.Current);

        var insertData = _meterReadingClient.BuildPostData(messageId.ToString(), command.ClientId,
            meterId, reading, readingOn);

        _meterReadingController.EntityContainer?.AddInsertMessageId(messageId);
        await _meterReadingClient.PostAsync(mqttSessionId, insertData);
    }

    private long GetParentEntityId(ConcurrentDictionary<Guid, long> collection, Guid? entityGuid)
    {
        if (!entityGuid.HasValue)
            return -1;

        return collection.GetValueOrDefault(entityGuid.Value, -1);
    }

    public async Task<string> ProcessAssetTaskPutAsync(Guid mqttSessionId, AssetTask assetTask)
    {
        assetTask.IsRfs = !assetTask.IsRfs;

        var namingStrategy = new CamelCaseNamingStrategy();
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = namingStrategy },
            Formatting = Formatting.Indented
        };

        string messageId = _assetTaskController.GetNextEntityMessageId;
        assetTask.MessageId = messageId;        
        assetTask.Description += "_Updated_" + messageId;

        var assetTaskJson = JsonConvert.SerializeObject(assetTask, jsonSerializerSettings);

        if (assetTask.Id != null)
        {
            _assetTaskController.EntityContainer?.AddUpdateMessageId(messageId);
            await _assetTaskClient.PutAsync(mqttSessionId, assetTask.Id.Value, assetTaskJson);
        }
        
        return messageId;
    }

    public async Task<string> ProcessAssetTaskDeleteAsync(Guid mqttSessionId, long assetTaskId)
    {
        string messageId = _assetTaskController.GetNextEntityMessageId;
        _assetTaskController.EntityContainer?.AddDeleteMessageId(messageId);
        await _assetTaskClient.DeleteAsync(assetTaskId, messageId, mqttSessionId);
        return messageId;
    }

    public async Task<string> ProcessMeterReadingPutAsync(Guid mqttSessionId, MeterReading meterReading)
    {
        var namingStrategy = new CamelCaseNamingStrategy();
        var jsonSerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = namingStrategy },
            Formatting = Formatting.Indented
        };

        string messageId = _meterReadingController.GetNextEntityMessageId;
        meterReading.MessageId = messageId;        
        meterReading.Reading += 10;

        var meterReadingJson = JsonConvert.SerializeObject(meterReading, jsonSerializerSettings);

        if (meterReading.Id != null)
        {
            _meterReadingController.EntityContainer?.AddUpdateMessageId(messageId);
            await _meterReadingClient.PutAsync(mqttSessionId, meterReading.Id.Value, meterReadingJson);
        }
        
        return messageId;
    }

    public async Task<string> ProcessMeterReadingDeleteAsync(Guid mqttSessionId, long meterReadingId)
    {
        string messageId = _meterReadingController.GetNextEntityMessageId;
        _meterReadingController.EntityContainer?.AddDeleteMessageId(messageId);
        await _meterReadingClient.DeleteAsync(meterReadingId, messageId, mqttSessionId);
        return messageId;
    }
}
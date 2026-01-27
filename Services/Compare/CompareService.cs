using Microsoft.Extensions.Hosting;
using MqttTestClient.Controllers;
using MqttTestClient.Models;
using MqttTestClient.Services.Web;
using Newtonsoft.Json;

namespace MqttTestClient.Services.Compare;

public interface ICompareService;

public class CompareService : BackgroundService, ICompareService
{
    private readonly AssetClient _assetClient;
    private readonly AssetController _assetController;
    private readonly AssetTaskClient _assetTaskClient;
    private readonly AssetTaskController _assetTaskController;
    private readonly List<string> _clientIds;
    private readonly bool _doAssets;
    private readonly bool _doAssetTasks;
    private readonly bool _doDelete;
    private readonly bool _doItems;
    private readonly bool _doMeterAdr;
    private readonly bool _doMeterReadings;
    private readonly bool _doMeters;
    private readonly int _entityCount;
    private readonly MeterClient _meterClient;
    private readonly MeterController _meterController;
    private readonly MeterReadingClient _meterReadingClient;
    private readonly MeterReadingController _meterReadingController;
    private readonly int _meterReadingsPerMeterCount;
    private readonly int _tasksPerAssetCount;
    private readonly TestEventContainer _testEventContainer;
    private readonly ToDoItemClient _toDoItemClient;
    private readonly ToDoItemController _toDoItemController;


    // ReSharper disable once ConvertToPrimaryConstructor
    public CompareService(List<string> clientIds,
        TestEventContainer testEventContainer, bool doItems, bool doAssets,
        bool doAssetTasks, ToDoItemController toDoItemController, AssetController assetController,
        AssetTaskController assetTaskController, AssetClient assetClient, AssetTaskClient assetTaskClient,
        ToDoItemClient toDoItemClient, bool doMeters, bool doMeterReadings, MeterController meterController,
        MeterClient meterClient,
        MeterReadingController meterReadingController, MeterReadingClient meterReadingClient,
        bool doDelete, int entityCount, bool doMeterAdr, int tasksPerAssetCount, int meterReadingsPerMeterCount)
    {
        _toDoItemController = toDoItemController;
        _clientIds = clientIds;
        _testEventContainer = testEventContainer;
        _doItems = doItems;
        _doAssets = doAssets;
        _doAssetTasks = doAssetTasks;
        _assetController = assetController;
        _assetTaskController = assetTaskController;
        _assetClient = assetClient;
        _assetTaskClient = assetTaskClient;
        _toDoItemClient = toDoItemClient;
        _meterController = meterController;
        _meterClient = meterClient;
        _doMeters = doMeters;
        _doMeterReadings = doMeterReadings;
        _meterReadingController = meterReadingController;
        _meterReadingClient = meterReadingClient;
        _doDelete = doDelete;
        _entityCount = entityCount;
        _doMeterAdr = doMeterAdr;
        _tasksPerAssetCount = tasksPerAssetCount;
        _meterReadingsPerMeterCount = meterReadingsPerMeterCount;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _testEventContainer.StartTestEvent.WaitOne();
        var result = true;
        var msg = string.Empty;

        Console.WriteLine("\r\nWaiting 1 second...");
        await Task.Delay(1000, stoppingToken);
        Console.WriteLine("\r\nCompare starting...");
        _testEventContainer.CompareTestStarted = true;
        var recordCounts = await GetLocalRecordCounts();

        var remoteItemCount = string.Empty;

        if (_doItems)
            remoteItemCount = await _toDoItemClient.GetRemoteItemCountAsync();

        var remoteAssetCount = string.Empty;

        if (_doAssets)
            remoteAssetCount = await _assetClient.GetRemoteAssetCountAsync();

        var remoteAssetTaskCount = string.Empty;

        if (_doAssetTasks)
            remoteAssetTaskCount = await _assetTaskClient.GetRemoteAssetTaskCountAsync();

        var remoteMeterCount = string.Empty;

        if (_doMeters)
            remoteMeterCount = await _meterClient.GetRemoteMeterCountAsync();

        var remoteMeterReadingCount = string.Empty;

        if (_doMeterReadings)
            remoteMeterReadingCount = await _meterReadingClient.GetRemoteMeterReadingCountAsync();

        foreach (var clientId in _clientIds)
        {
            Console.WriteLine("");
            Console.WriteLine("Client Id: " + clientId);

            if (_doItems)
            {
                var itemResult =
                    await CompareItemsAsync(clientId, recordCounts.itemCount, Convert.ToInt32(remoteItemCount));
                result = result && itemResult.result;
                msg = itemResult.msg;
                Console.WriteLine("Items done..." + msg);
            }

            if (_doAssets)
            {
                var assetResult = await CompareAssetsAsync(clientId, recordCounts.assetCount,
                    Convert.ToInt32(remoteAssetCount));
                result = result && assetResult.result;
                msg = assetResult.msg;
                Console.WriteLine("Assets done..." + msg);
            }

            if (_doAssetTasks)
            {
                var assetTaskResult =
                    await CompareAssetTasksAsync(clientId, recordCounts.assetTaskCount,
                        Convert.ToInt32(remoteAssetTaskCount));
                result = result && assetTaskResult.result;
                msg = assetTaskResult.msg;
                Console.WriteLine("Tasks done..." + msg);
            }

            if (_doMeters)
            {
                var meterResult = await CompareMetersAsync(clientId, recordCounts.meterCount,
                    Convert.ToInt32(remoteMeterCount));
                result = result && meterResult.result;
                msg = meterResult.msg;
                Console.WriteLine("Meters done..." + msg);
            }

            if (_doMeterReadings)
            {
                var meterReadingResult = await CompareMeterReadingsAsync(clientId, recordCounts.meterReadingCount,
                    Convert.ToInt32(remoteMeterReadingCount));
                result = result && meterReadingResult.result;
                msg = meterReadingResult.msg;
                Console.WriteLine("Meter Readings done..." + msg);
            }
        }

        _testEventContainer.Stopwatch.Stop();
        Console.WriteLine("");

        if (result)
            Console.WriteLine("Test successful...");
        else
            Console.WriteLine("Test failed..." + msg);

        var elapsedSeconds = _testEventContainer.Stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine("Elapsed Time: " + elapsedSeconds + " seconds");
    }

    private async Task<(bool result, string msg)> CompareItemsAsync(string clientId, int localCount, int remoteCount)
    {
        var result = (true, string.Empty);

        if (_doDelete)
        {
            var toDoDeletedList = _testEventContainer.GetAllDeletedParentEntityLists<ToDoItem>();
            var toDoIgnoredList = _testEventContainer.GetAllIgnoredParentEntityLists<ToDoItem>();            
            int? expectedCount = _entityCount * _clientIds.Count - toDoDeletedList.Count - toDoIgnoredList.Count;
            
            if (Convert.ToInt32(remoteCount) != expectedCount)
            {
                var msg = $"Remote Item count should be {expectedCount}..." + Environment.NewLine;
                msg += "Client Id: " + clientId + Environment.NewLine;
                msg += "Remote Count: " + remoteCount + Environment.NewLine;
                result = (false, msg);
                return result;
            }
        }
        else if (!_doDelete & (remoteCount != localCount))
        {
            var msg = "Item count different..." + Environment.NewLine;
            msg += "Client Id: " + clientId + Environment.NewLine;
            msg += "Remote Item Count: " + remoteCount + Environment.NewLine;
            msg += "Local Item Count: " + localCount + Environment.NewLine;
            result = (false, msg);
            return result;
        }

        var insertQueue = _testEventContainer.GetCreateQueue<ToDoItem>(clientId);
        var testQueue = new Queue<ToDoItem>();

        foreach (var item in insertQueue) testQueue.Enqueue(item);

        if (testQueue.Count <= 0)
        {
            result = (true, "No Items to test...");
            return result;
        }

        Console.WriteLine($"Testing {testQueue.Count} Items...");

        while (testQueue.Count > 0)
        {
            var toDoItem = testQueue.Dequeue();
            var localTodoItem = await _toDoItemController.GetItemById(Convert.ToInt64(toDoItem.Id));

            if (localTodoItem is not { Id: not null })
                continue;

            var remoteEntity = await _toDoItemClient.GetItemAsync(localTodoItem.Id.Value);

            if (remoteEntity == "null")
                continue;
            
            var remoteToDoItem =
                (ToDoItem)(JsonConvert.DeserializeObject(remoteEntity, typeof(ToDoItem)) ?? string.Empty);

            if (localTodoItem.Version != remoteToDoItem.Version)
            {
                var msg = "Item Version mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localTodoItem.Name != remoteToDoItem.Name)
            {
                var msg = "Item Name mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localTodoItem.Description != remoteToDoItem.Description)
            {
                var msg = "Item Description mismatch..." + localTodoItem.Id;
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localTodoItem.IsComplete != remoteToDoItem.IsComplete)
            {
                var msg = "Item IsComplete mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localTodoItem.ClientId != remoteToDoItem.ClientId)
            {
                var msg = "Item ClientId mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }
        }

        return result;
    }

    private async Task<(bool result, string msg)> CompareAssetsAsync(string clientId, int localCount, int remoteCount)
    {
        var result = (true, string.Empty);

        if (_doDelete)
        {
            var assetDeletedList = _testEventContainer.GetAllDeletedParentEntityLists<Asset>();
            var assetIgnoredList = _testEventContainer.GetAllIgnoredParentEntityLists<Asset>();            
            int? expectedCount = _entityCount * _clientIds.Count - assetDeletedList.Count - assetIgnoredList.Count;

            if (Convert.ToInt32(remoteCount) != expectedCount)
            {
                var msg = $"Remote Asset count should be {expectedCount}..." + Environment.NewLine;
                msg += "Client Id: " + clientId + Environment.NewLine;
                msg += "Remote Count: " + remoteCount + Environment.NewLine;
                result = (false, msg);
                return result;
            }
        }
        else if (!_doDelete & (Convert.ToInt32(remoteCount) != localCount))
        {
            var msg = "Asset count different..." + Environment.NewLine;
            msg += "Client Id: " + clientId + Environment.NewLine;
            msg += "Remote Asset Count: " + remoteCount + Environment.NewLine;
            msg += "Local Asset Count: " + localCount + Environment.NewLine;
            result = (false, msg);
            return result;
        }

        var insertQueue = _testEventContainer.GetCreateQueue<Asset>(clientId);
        var testQueue = new Queue<Asset>();

        foreach (var item in insertQueue) testQueue.Enqueue(item);

        if (testQueue.Count <= 0)
        {
            result = (true, "No Assets to test...");
            return result;
        }

        Console.WriteLine($"Testing {testQueue.Count} Assets...");

        while (testQueue.Count > 0)
        {
            var asset = testQueue.Dequeue();
            var localAsset = await _assetController.GetAssetByIdAsync(Convert.ToInt64(asset.Id));

            if (localAsset is not { Id: not null }) continue;

            var remoteEntity = await _assetClient.GetRemoteAssetAsync(localAsset.Id.Value);

            if (remoteEntity == "null")
                continue;
            
            var remoteAsset =
                (Asset)(JsonConvert.DeserializeObject(remoteEntity, typeof(Asset)) ?? string.Empty);

            if (localAsset.Version != remoteAsset.Version)
            {
                var msg = "Asset Version mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAsset.Code != remoteAsset.Code)
            {
                var msg = "Asset Code mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAsset.Description != remoteAsset.Description)
            {
                var msg = "Asset Description mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAsset.IsMsi != remoteAsset.IsMsi)
            {
                var msg = "Item IsMsi mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAsset.ClientId != remoteAsset.ClientId)
            {
                var msg = "Item ClientId mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }
        }

        return result;
    }

    private async Task<(bool result, string msg)> CompareAssetTasksAsync(string clientId, int localCount,
        int remoteCount)
    {
        var result = (true, string.Empty);

        if (_doDelete)
        {
            var assetTaskDeletedList = _testEventContainer.GetAllDeletedChildEntityLists<AssetTask>();
            var assetTaskIgnoredList = _testEventContainer.GetAllIgnoredChildEntityLists<AssetTask>();            
            int? expectedCount = _entityCount * _tasksPerAssetCount * _clientIds.Count - assetTaskDeletedList.Count -
                                 assetTaskIgnoredList.Count;

            if (Convert.ToInt32(remoteCount) != expectedCount)
            {
                var msg = $"Remote Asset Task count should be {expectedCount}..." + Environment.NewLine;
                msg += "Client Id: " + clientId + Environment.NewLine;
                msg += "Remote Count: " + remoteCount + Environment.NewLine;
                result = (false, msg);
                return result;
            }
        }
        else if (Convert.ToInt32(remoteCount) != localCount)
        {
            var msg = "Asset Task count different..." + Environment.NewLine;
            msg += "Client Id: " + clientId + Environment.NewLine;
            msg += "Remote Count: " + remoteCount + Environment.NewLine;
            msg += "Local Count: " + localCount + Environment.NewLine;
            result = (false, msg);
            return result;
        }

        var insertQueue = _testEventContainer.GetCreateQueue<AssetTask>(clientId);
        var testQueue = new Queue<AssetTask>();

        foreach (var item in insertQueue) testQueue.Enqueue(item);

        if (testQueue.Count <= 0)
        {
            result = (true, "No Tasks to test...");
            return result;
        }

        Console.WriteLine($"Testing {testQueue.Count} Tasks...");

        while (testQueue.Count > 0)
        {
            var assetTask = testQueue.Dequeue();
            var localAssetTask =
                await _assetTaskController.GetAssetTask(Convert.ToInt64(assetTask.Id));

            if (localAssetTask is not { Id: not null }) continue;

            var remoteEntity = await _assetTaskClient.GetRemoteAssetTaskAsync(localAssetTask.Id.Value);

            if (remoteEntity == "null")
                continue;
            
            var remoteAssetTask =
                (AssetTask)(JsonConvert.DeserializeObject(remoteEntity, typeof(AssetTask)) ?? string.Empty);

            if (localAssetTask.Version != remoteAssetTask.Version)
            {
                var msg = "Asset Task Version mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAssetTask.Code != remoteAssetTask.Code)
            {
                var msg = "Asset Task Code mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAssetTask.Description != remoteAssetTask.Description)
            {
                var msg = "Asset Task Description mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localAssetTask.IsRfs != remoteAssetTask.IsRfs)
            {
                var msg = "Item IsRfs mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }
        }

        return result;
    }

    private async Task<(bool result, string msg)> CompareMetersAsync(string clientId, int localCount, int remoteCount)
    {
        var result = (true, string.Empty);

        if (_doDelete)
        {
            var meterDeletedList = _testEventContainer.GetAllDeletedParentEntityLists<Meter>();
            var meterIgnoredList = _testEventContainer.GetAllIgnoredParentEntityLists<Meter>();            
            int? expectedCount = _entityCount * _clientIds.Count - meterDeletedList.Count - meterIgnoredList.Count;

            if (Convert.ToInt32(remoteCount) != expectedCount)
            {
                var msg = $"Remote Meter count should be {expectedCount}..." + Environment.NewLine;
                msg += "Client Id: " + clientId + Environment.NewLine;
                msg += "Remote Count: " + remoteCount + Environment.NewLine;
                result = (false, msg);
                return result;
            }
        }
        else if (!_doDelete & (Convert.ToInt32(remoteCount) != localCount))
        {
            var msg = "Item count different..." + Environment.NewLine;
            msg += "Client Id: " + clientId + Environment.NewLine;
            msg += "Remote Count: " + remoteCount + Environment.NewLine;
            msg += "Local Count: " + localCount + Environment.NewLine;
            result = (false, msg);
            return result;
        }

        var insertQueue = _testEventContainer.GetCreateQueue<Meter>(clientId);
        var testQueue = new Queue<Meter>();

        foreach (var item in insertQueue) testQueue.Enqueue(item);

        if (testQueue.Count <= 0)
        {
            result = (true, "No Meters to test...");
            return result;
        }

        Console.WriteLine($"Testing {testQueue.Count} Meters...");

        while (testQueue.Count > 0)
        {
            var meter = testQueue.Dequeue();
            var localMeter = await _meterController.GetMeter(Convert.ToInt64(meter.Id));

            if (localMeter is not { Id: not null }) continue;

            var remoteEntity = await _meterClient.GetRemoteMeterAsync(localMeter.Id.Value);

            if (remoteEntity == "null")
                continue;
            
            var remoteMeter =
                (Meter)(JsonConvert.DeserializeObject(remoteEntity, typeof(Meter)) ?? string.Empty);

            if (_doMeterAdr)
            {
                var localMeterAdr = await _meterController.GetMeterAdrAsync(Convert.ToInt64(meter.Id));
                var remoteMeterAdr = await _meterClient.GetRemoteMeterAdrAsync(Convert.ToInt64(meter.Id));

                if ($"{localMeterAdr:0.00}" != $"{remoteMeterAdr:0.00}")
                {
                    var msg = "Meter Adr mismatch...";
                    Console.WriteLine(msg);
                    Console.WriteLine("local: " + $"{localMeterAdr:0.00}");
                    Console.WriteLine("remote: " + $"{remoteMeterAdr:0.00}");
                    result = (false, msg);
                }
            }

            if (localMeter.Version != remoteMeter.Version)
            {
                var msg = "Meter Version mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localMeter.Code != remoteMeter.Code)
            {
                var msg = "Meter Code mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localMeter.Description != remoteMeter.Description)
            {
                var msg = "Meter Description mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localMeter.IsPaused != remoteMeter.IsPaused)
            {
                var msg = "Meter IsPaused mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localMeter.ClientId != remoteMeter.ClientId)
            {
                var msg = "Item ClientId mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }
        }

        return result;
    }

    private async Task<(bool result, string msg)> CompareMeterReadingsAsync(string clientId, int localCount,
        int remoteCount)
    {
        var result = (true, string.Empty);

        if (_doDelete)
        {
            var meterReadingDeletedList = _testEventContainer.GetAllDeletedChildEntityLists<MeterReading>();
            var meterReadingIgnoredList = _testEventContainer.GetAllIgnoredChildEntityLists<MeterReading>();            
            int? expectedCount = _entityCount * _meterReadingsPerMeterCount * _clientIds.Count -
                                 meterReadingDeletedList.Count - meterReadingIgnoredList.Count;

            if (Convert.ToInt32(remoteCount) != expectedCount)
            {
                var msg = $"Remote Meter Reading count should be {expectedCount}..." + Environment.NewLine;
                msg += "Client Id: " + clientId + Environment.NewLine;
                msg += "Remote Count: " + remoteCount + Environment.NewLine;
                result = (false, msg);
                return result;
            }
        }
        else if (!_doDelete & (Convert.ToInt32(remoteCount) != localCount))
        {
            var msg = "Meter Reading count different..." + Environment.NewLine;
            msg += "Client Id: " + clientId + Environment.NewLine;
            msg += "Remote Count: " + remoteCount + Environment.NewLine;
            msg += "Local Count: " + localCount + Environment.NewLine;
            result = (false, msg);
            return result;
        }

        var insertQueue = _testEventContainer.GetCreateQueue<MeterReading>(clientId);
        var testQueue = new Queue<MeterReading>();

        foreach (var item in insertQueue) testQueue.Enqueue(item);

        if (testQueue.Count <= 0)
        {
            result = (true, "No Meter Readings to test...");
            return result;
        }

        Console.WriteLine($"Testing {testQueue.Count} Readings...");

        while (testQueue.Count > 0)
        {
            var meterReading = testQueue.Dequeue();
            var localMeterReading =
                await _meterReadingController.GetLocalMeterReadingByIdAsync(Convert.ToInt64(meterReading.Id));

            if (localMeterReading == null)
                continue;

            var remoteEntity = await _meterReadingClient.GetRemoteMeterReadingAsync(localMeterReading.Id.Value);
            
            if (remoteEntity == "null")
                continue;
            
            MeterReading remoteMeterReading; 

            try
            {
                remoteMeterReading =
                    (MeterReading)(JsonConvert.DeserializeObject(remoteEntity, typeof(MeterReading)) ?? string.Empty);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            if (localMeterReading.Version != remoteMeterReading.Version)
            {
                var msg = "Meter Reading Version mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if (localMeterReading.MeterId != remoteMeterReading.MeterId)
            {
                var msg = "Meter Id mismatch...";
                Console.WriteLine(msg);
                result = (false, msg);
            }

            if ($"{localMeterReading.Reading:0.0000}" != $"{remoteMeterReading.Reading:0.0000}")
            {
                var msg = "Meter Reading mismatch...";
                Console.WriteLine(msg);
                Console.WriteLine("local: " + $"{localMeterReading.Reading:0.0000}");
                Console.WriteLine("remote: " + $"{remoteMeterReading.Reading:0.0000}");
                Console.WriteLine("local id: " + $"{localMeterReading.Id}");
                Console.WriteLine("remote id: " + $"{remoteMeterReading.Id}");

                result = (false, msg);
            }

            if (localMeterReading.ReadingOn?.ToString("MM-dd-yyyy HH:mm:ss") !=
                remoteMeterReading.ReadingOn?.ToString("MM-dd-yyyy HH:mm:ss"))
            {
                var msg = "Meter ReadingOn mismatch...";
                Console.WriteLine(msg);
                Console.WriteLine("Local: " + localMeterReading.ReadingOn?.ToString("MM-dd-yyyy HH:mm:ss"));
                Console.WriteLine("Remote: " + remoteMeterReading.ReadingOn?.ToString("MM-dd-yyyy HH:mm:ss"));
                Console.WriteLine("local id: " + $"{localMeterReading.Id}");
                Console.WriteLine("remote id: " + $"{remoteMeterReading.Id}");
            
                result = (false, msg);
            }
        }

        return result;
    }

    private async Task<(int itemCount, int assetCount, int assetTaskCount, int meterCount, int meterReadingCount)>
        GetLocalRecordCounts()
    {
        var assetCount = 0;
        var itemCount = 0;
        var assetTaskCount = 0;
        var meterCount = 0;
        var meterReadingCount = 0;
        
        if (_doAssets)
            assetCount = await _assetController.GetLocalAssetCount();
        
        if (_doItems)
            itemCount = await _toDoItemController.GetLocalItemCount();
        
        if (_doAssetTasks)
            assetTaskCount = await _assetTaskController.GetLocalAssetTaskCount();
        
        if (_doMeters)
            meterCount = await _meterController.GetLocalMeterCountAsync();
        
        if (_doMeterReadings)
            meterReadingCount = await _meterReadingController.GetLocalMeterReadingCountAsync();

        return (itemCount, assetCount, assetTaskCount, meterCount, meterReadingCount);
    }
}
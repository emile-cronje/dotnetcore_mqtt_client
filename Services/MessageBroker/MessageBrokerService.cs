using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using MQTTnet;
using MQTTnet.Protocol;
using MqttTestClient.Controllers;
using MqttTestClient.Models;
using Newtonsoft.Json;
using Meter = MqttTestClient.Models.Meter;

// ReSharper disable All

namespace MqttTestClient.Services.MessageBroker;

public interface IMessageBrokerService;

public class MessageBrokerService : BackgroundService, IMessageBrokerService
{
    private readonly int _entityUpdateCount;
    private readonly int _finalEntityVersion;
    private EntityContainer? _assetContainer;
    private AssetController _assetController;
    private EntityContainer? _assetTaskContainer;
    private AssetTaskController _assetTaskController;
    private EntityCrudContainer _entityCrudContainer;
    private EntityContainer? _meterContainer;
    private MeterController _meterController;
    private EntityContainer? _meterReadingContainer;
    private MeterReadingController _meterReadingController;
    private int _meterReadingsPerMeterCount;
    private int _tasksPerAssetCount;    
    private List<(string, string)> _mqttBrokers;
    private TestEventContainer _testEventContainer;
    private EntityContainer? _toDoContainer;
    private ToDoItemController _toDoController;
    private IConsumer<Ignore, string> _consumer = null!;    
    private readonly Guid _mqttSessionId;

    public MessageBrokerService(List<(string broker, string clientId)> mqttBrokers,
        int entityUpdateCount, int meterReadingsPerMeterCount,
        TestEventContainer testEventContainer,
        ToDoItemController toDoController, AssetController assetController,
        AssetTaskController assetTaskController, MeterController meterController,
        MeterReadingController meteterReadingController,
        EntityCrudContainer entityCrudContainer,
        int tasksPerAssetCount, Guid mqttSessionId)
    {
        _mqttBrokers = mqttBrokers;
        _entityUpdateCount = entityUpdateCount;
        _finalEntityVersion = _entityUpdateCount;
        _testEventContainer = testEventContainer;
        _toDoController = toDoController;
        _assetController = assetController;
        _assetTaskController = assetTaskController;
        _meterController = meterController;
        _meterReadingController = meteterReadingController;
        _meterReadingsPerMeterCount = meterReadingsPerMeterCount;
        _entityCrudContainer = entityCrudContainer;
        _tasksPerAssetCount = tasksPerAssetCount;
        _mqttSessionId = mqttSessionId;                    
    }

    protected override async Task ExecuteAsync(CancellationToken stopToken)
    {
        Initialise();
        
        await RunMqttConsumers(_mqttSessionId, _mqttBrokers);
//        RunKafkaConsumers();

        RunMonitorEventQueue();
       
        await Task.CompletedTask;        
    }

    private void MonitorEventQueue()
    {
        var task = MonitorEventQueueConsumer();
        task.GetAwaiter().GetResult();
    }
    
    private async Task MonitorEventQueueConsumer()
    {
        _testEventContainer.StartCompareMonitorEvent.WaitOne();                                        

        while (true)
        {
            Dictionary<EntityType, int> insertCountsPerEntityType = _testEventContainer.EntityContainers
                .Where(x => x.Value != null)
                .GroupBy(x => x.Key)
                .ToDictionary(
                    group => group.Key,
                    group => group.Sum(x => x.Value?.GetInsertMessageIdsCount() ?? 0)
                );

            foreach (var kvp in insertCountsPerEntityType)
            {
                if (kvp.Value > 0)
                    Console.WriteLine($"Insert: EntityType: {kvp.Key}, Count: {kvp.Value}");
            }

            Dictionary<EntityType, int> updateCountsPerEntityType = _testEventContainer.EntityContainers
                .Where(x => x.Value != null)
                .GroupBy(x => x.Key)
                .ToDictionary(
                    group => group.Key,
                    group => group.Sum(x => x.Value?.GetUpdateMessageIdsCount() ?? 0)
                );

            foreach (var kvp in updateCountsPerEntityType)
            {
                if (kvp.Value > 0)                
                    Console.WriteLine($"Update: EntityType: {kvp.Key}, Count: {kvp.Value}");
            }

            Dictionary<EntityType, int> deleteCountsPerEntityType = _testEventContainer.EntityContainers
                .Where(x => x.Value != null)
                .GroupBy(x => x.Key)
                .ToDictionary(
                    group => group.Key,
                    group => group.Sum(x => x.Value?.GetDeleteMessageIdsCount() ?? 0)
                );

            foreach (var kvp in deleteCountsPerEntityType)
            {
                if (kvp.Value > 0)                
                    Console.WriteLine($"Delete: EntityType: {kvp.Key}, Count: {kvp.Value}");
            }

            int allInsertMessageIdsCount =
                _testEventContainer.EntityContainers.Sum(x =>
                {
                    if (x.Value != null) return x.Value.GetInsertMessageIdsCount();
                    return 0;
                });

            int allUpdateMessageIdsCount =
                _testEventContainer.EntityContainers.Sum(x =>
                {
                    if (x.Value != null) return x.Value.GetUpdateMessageIdsCount();
                    return 0;
                });
            
            int allDeleteMessageIdsCount =
                _testEventContainer.EntityContainers.Sum(x =>
                {
                    if (x.Value != null) return x.Value.GetDeleteMessageIdsCount();
                    return 0;
                });

            bool allInsertsCompleted =
                _testEventContainer.EntityContainers.All(x =>
                    x.Value != null && x.Value.HasInsertSendCompleted);
            bool allDeletesCompleted =
                _testEventContainer.EntityContainers.All(x =>
                    x.Value != null && x.Value.HasDeleteSendCompleted);
            bool allUpdatesCompleted =
                _testEventContainer.EntityContainers.All(x =>
                    x.Value != null && x.Value.HasUpdateSendCompleted);

            if (allInsertMessageIdsCount > 0)
                Console.WriteLine("insertMessageIdsCount: " + allInsertMessageIdsCount);

            if (allUpdateMessageIdsCount > 0)                
                Console.WriteLine("updateMessageIdsCount: " + allUpdateMessageIdsCount);

            if (allDeleteMessageIdsCount > 0)                
                Console.WriteLine("deleteMessageIdsCount: " + allDeleteMessageIdsCount);

            bool printDebug = false;
            
            if (printDebug && (_entityCrudContainer.DoInsert) && (allInsertMessageIdsCount < 4))
            {
                foreach (var kvp in _testEventContainer.EntityContainers)
                {
                    var entityType = kvp.Key;
                    var container = kvp.Value;

                    if (container != null)
                    {
                        var containerInsertMessageCount = container.GetInsertMessageIdsCount();
                        
                        if (containerInsertMessageCount < 4)
                        {
                            var insertMessageIds = container.GetInsertMessageIds().ToList();

                            foreach (var messageId in insertMessageIds)
                            {
                                Console.WriteLine($"Insert MessageId: {messageId}");
                            }
                            
                            var sentMessageIds = container.GetSentMessageIds().ToList();

                            foreach (var messageId in sentMessageIds)
                            {
                                Console.WriteLine($"Insert Sent MessageId: {messageId}");
                            }
                            
                            var receivedMessageIds = container.GetReceivedMessageIds().ToList();

                            foreach (var messageId in receivedMessageIds)
                            {
                                Console.WriteLine($"Insert Received MessageId: {messageId}");
                            }
                        }
                    }
                }
            }

            if (printDebug && (_entityCrudContainer.DoUpdate) && (allUpdateMessageIdsCount < 4))                
            {
                foreach (var kvp in _testEventContainer.EntityContainers)
                {
                    var entityType = kvp.Key;
                    var container = kvp.Value;

                    if (container != null)
                    {
                        var containerUpdateMessageCount = container.GetUpdateMessageIdsCount();
                        
                        if (containerUpdateMessageCount < 4)
                        {
                            var updateMessageIds = container.GetUpdateMessageIds().ToList();                            

                            foreach (var messageId in updateMessageIds)
                            {
                                Console.WriteLine($"Update MessageId: {messageId}");
                            }
                            
                            var sentMessageIds = container.GetSentMessageIds().ToList();

                            foreach (var messageId in sentMessageIds)
                            {
                                Console.WriteLine($"Update Sent MessageId: {messageId}");
                            }
                            
                            var receivedMessageIds = container.GetReceivedMessageIds().ToList();

                            foreach (var messageId in receivedMessageIds)
                            {
                                Console.WriteLine($"Update Received MessageId: {messageId}");
                            }
                        }
                    }
                }
            }

            if (printDebug && (_entityCrudContainer.DoDelete) && (allDeleteMessageIdsCount < 4))                
            {
                foreach (var kvp in _testEventContainer.EntityContainers)
                {
                    var entityType = kvp.Key;
                    var container = kvp.Value;

                    if (container != null)
                    {
                        var containerDeleteMessageCount = container.GetDeleteMessageIdsCount();
                        
                        if (containerDeleteMessageCount < 4)
                        {
                            var deleteMessageIds = container.GetDeleteMessageIds().ToList();                            

                            foreach (var messageId in deleteMessageIds)
                            {
                                Console.WriteLine($"Delete MessageId: {messageId}");
                            }
                            
                            var sentMessageIds = container.GetSentMessageIds().ToList();

                            foreach (var messageId in sentMessageIds)
                            {
                                Console.WriteLine($"Delete Sent MessageId: {messageId}");
                            }
                            
                            var receivedMessageIds = container.GetReceivedMessageIds().ToList();

                            foreach (var messageId in receivedMessageIds)
                            {
                                Console.WriteLine($"Delete Received MessageId: {messageId}");
                            }
                        }
                    }
                }
            }
            
            // Insert
            if (_entityCrudContainer.DoInsert && !_entityCrudContainer.DoUpdate && !_entityCrudContainer.DoDelete)
            {
                if (allInsertsCompleted && allInsertMessageIdsCount == 0)
                {
                    _testEventContainer.AllInsertsCompletedEvent.Set();
                }

                if (allInsertsCompleted && allInsertMessageIdsCount == 0)
                {
                    ShouldTestProcessStart();                    
                    return;
                }
            }
            // Insert, Update                
            else if (_entityCrudContainer.DoInsert && _entityCrudContainer.DoUpdate &&
                     !_entityCrudContainer.DoDelete)
            {
                if (allInsertsCompleted && allInsertMessageIdsCount == 0)
                {
                    _testEventContainer.AllInsertsCompletedEvent.Set();
                }

                if (allInsertsCompleted && allInsertMessageIdsCount == 0
                                        && allUpdatesCompleted && allUpdateMessageIdsCount == 0)
                {
                    ShouldTestProcessStart();                                        
                    return;
                }
            }
            // Insert, Delete                
            else if (_entityCrudContainer.DoInsert && !_entityCrudContainer.DoUpdate &&
                     _entityCrudContainer.DoDelete)
            {
                if (allInsertsCompleted && allInsertMessageIdsCount == 0)
                {
                    _testEventContainer.AllInsertsCompletedEvent.Set();
                }

                if (allInsertsCompleted && allInsertMessageIdsCount == 0 && allDeletesCompleted &&
                    allDeleteMessageIdsCount == 0)
                {
                    ShouldTestProcessStart();                                        
                    return;
                }
            }
            // Insert, Update, Delete                
            else if (_entityCrudContainer.DoInsert && _entityCrudContainer.DoUpdate &&
                     _entityCrudContainer.DoDelete)
            {
                if (allInsertsCompleted && allInsertMessageIdsCount == 0)
                {
                    _testEventContainer.AllInsertsCompletedEvent.Set();
                }

                if (allInsertsCompleted && allInsertMessageIdsCount == 0
                                        && allUpdatesCompleted && allUpdateMessageIdsCount == 0
                                        && allDeletesCompleted && allDeleteMessageIdsCount == 0)
                {
                    ShouldTestProcessStart();                                        
                    return;
                }
            }
            
            Thread.Sleep(1000);
            await Task.CompletedTask;            
        }
    }
    
    private void ShouldTestProcessStart()
    {
        _testEventContainer.StartTestEvent.Set();
    }

    private void Initialise()
    {
        if (_testEventContainer.EntityContainers.ContainsKey(EntityType.ToDoItem))
            _toDoContainer = _testEventContainer.EntityContainers[EntityType.ToDoItem];

        if (_testEventContainer.EntityContainers.ContainsKey(EntityType.Asset))
            _assetContainer = _testEventContainer.EntityContainers[EntityType.Asset];

        if (_testEventContainer.EntityContainers.ContainsKey(EntityType.AssetTask))
            _assetTaskContainer = _testEventContainer.EntityContainers[EntityType.AssetTask];

        if (_testEventContainer.EntityContainers.ContainsKey(EntityType.Meter))
            _meterContainer = _testEventContainer.EntityContainers[EntityType.Meter];

        if (_testEventContainer.EntityContainers.ContainsKey(EntityType.MeterReading))
            _meterReadingContainer = _testEventContainer.EntityContainers[EntityType.MeterReading];
    }

    private async Task RunMqttConsumers(Guid mqttSessionId, List<(string broker, string clientId)> mqttBrokers)
    {
        foreach (var broker in mqttBrokers)
        {
            await RunMqttConsumer(mqttSessionId, broker.broker, broker.clientId);
        }
    }

    private void RunKafkaConsumers()
    {
        var thread = new Thread(RunKafkaConsumer) { IsBackground = true };
        thread.Start();        
    }
    
    private async Task RunMqttConsumer(Guid mqttSessionId, string mqttBroker, string mqttClientId)
    {
        var reconnect_interval = 5;
        var mqttClient = new MqttClientFactory().CreateMqttClient();

        try
        {
            var options = new MqttClientOptionsBuilder()
                .WithClientId(mqttClientId + Guid.NewGuid())
                .WithTcpServer(mqttBroker)
                .WithCleanSession()
                .Build();

            await mqttClient.ConnectAsync(options);

            await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                .WithTopic("/entities")
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build());

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                if (e.ApplicationMessage.Topic == "/entities")
                {
                    string payload = e.ApplicationMessage.ConvertPayloadToString();

                    if (payload != null)
                    {
                        string message = payload;
                        await ProcessMessage(mqttSessionId, message);                        
                    }
                }
            };
        }
        catch (Exception error)
        {
            Console.WriteLine($"Error \"{error}\". Reconnecting in {reconnect_interval} seconds.");
            await Task.Delay(TimeSpan.FromSeconds(reconnect_interval));
        }
    }

    private void RunKafkaConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "my-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        _consumer.Subscribe("entities");
        
        var task = Consume();
        task.GetAwaiter().GetResult();
    }

    private void RunMonitorEventQueue()
    {
        var thread = new Thread(MonitorEventQueue) { IsBackground = true };
        thread.Start();        
    }
    
    private async Task Consume()
    {
        try
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // Prevent the process from terminating immediately
                cts.Cancel();
            };
            
            while (true)
            {
                try
                {
                    var consumerResult = _consumer.Consume(cts.Token);

                    if (consumerResult.IsPartitionEOF)
                        continue;

                    _ = Task.Run(async () => 
                    {
                        try
                        {
                            await ProcessMessage(Guid.NewGuid(), consumerResult.Message.Value);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing message: {ex.Message}");
                        }
                    });                    

                    _consumer.Commit(consumerResult);                    
                }
                catch (OperationCanceledException)
                {
                    _consumer.Close();
                }
                catch (KafkaException ex)
                {
                    Console.WriteLine($"Consume error: {ex.Error.Reason}");
                }
                
                await Task.CompletedTask;
            }
        }
        finally
        {
            Dispose();
        }
    }
   
    public async Task ProcessMessage(Guid mqttSessionId, string message)
    {
        if (message != null)
        {
            EntityMetaData entityMetaData =
                (EntityMetaData)(JsonConvert.DeserializeObject(message, typeof(EntityMetaData)) ??
                                 string.Empty);

            if (entityMetaData == null)
                return;

            var entityType = entityMetaData.EntityType;
            var recvMqttSessionId = entityMetaData.MqttSessionId;

            if (entityType == "Asset")
                await ProcessMqttAssetMessageAsync(entityMetaData);
            else if (entityType == "ToDoItem")
                await ProcessMqttItemMessageAsync(mqttSessionId, entityMetaData);
            else if (entityType == "AssetTask")
                await ProcessMqttAssetTaskMessageAsync(entityMetaData);
            else if (entityType == "Meter")
                await ProcessMqttMeterMessageAsync(entityMetaData);
            else if (entityType == "MeterReading")
                await ProcessMqttMeterReadingMessageAsync(entityMetaData);
        }
    }

    private async Task ProcessMqttItemMessageAsync(Guid mqttSessionId, EntityMetaData itemMetaData)
    {
        var operation = itemMetaData.Operation;
        var clientId = itemMetaData.ClientId;

        if ((itemMetaData.Entity == null) && (clientId == null) && (itemMetaData.EntityId == null))
            return;

        if ((itemMetaData.Entity == null) || (mqttSessionId != itemMetaData.MqttSessionId))
        {
            if (operation == "Update")
                _toDoContainer?.RemoveUpdateMessageId(itemMetaData.MessageId);
            else if (operation == "Delete")
                _toDoContainer?.RemoveDeleteMessageId(itemMetaData.MessageId);
            else if (operation == "Create")
            {
                if (clientId != null)
                    _testEventContainer.IgnoredParentEntityList.AddItem(clientId, new ToDoItem(Convert.ToInt32(clientId))
                    {
                        Name = null,
                        Description = null
                    });

                _toDoContainer?.RemoveInsertMessageId(itemMetaData.MessageId);                
            }
        }
        else
        {
            ToDoItem toDoItem =
                (ToDoItem)(JsonConvert.DeserializeObject(itemMetaData.Entity, typeof(ToDoItem)) ?? string.Empty);

            if (toDoItem == null)
                return;

            if (_testEventContainer.CompareTestStarted == false)
                Console.WriteLine($"{operation}: {nameof(ToDoItem)}: {toDoItem.Id}, {toDoItem.Name}, {clientId}");

            if (operation == "Create")
            {
                if (clientId != null)
                {
                    if (_toDoContainer != null)
                        await _toDoController.AddItemAsync(toDoItem);
                    
                    _testEventContainer.CreateQueueManager.Enqueue(clientId, toDoItem);

                    if (toDoItem.Id != null)
                    {
                        _testEventContainer.AddEntity(EntityType.ToDoItem, toDoItem.Id.Value);
                    }
                }
            }
            else if (operation == "Update")
            {
                if (clientId != null)
                {
                    if (_toDoContainer != null)
                    {
                        await _toDoController.UpdateItemAsync(toDoItem);                        
                    }
                    
                    int dbVersion = await _toDoController.GetItemVersion(Convert.ToInt64(toDoItem.Id));

                    if (dbVersion == _finalEntityVersion)
                    {
                        _testEventContainer.UpdateQueueManager.Enqueue(clientId, toDoItem);
                    }
                }
            }
            else if (operation == "Delete")
            {
                if (clientId != null)
                {
                    if (itemMetaData.Entity != null)
                    {
                        toDoItem = (ToDoItem)(JsonConvert.DeserializeObject(itemMetaData.Entity, typeof(ToDoItem)) ??
                                              string.Empty);

                        if (toDoItem.Id != null)
                        {
                            await _toDoController.DeleteItemAsync(toDoItem, itemMetaData.MessageId);
                            _testEventContainer.DeletedParentEntityList.AddItem(clientId, toDoItem);
                            _testEventContainer.RemoveEntityId(EntityType.ToDoItem, toDoItem.Id.Value);
                        }
                    }
                }
            }
        }
    }

    private async Task ProcessMqttAssetMessageAsync(EntityMetaData assetMetaData)
    {
        var operation = assetMetaData.Operation;
        var clientId = assetMetaData.ClientId;

        if ((assetMetaData.Entity == null) && (clientId == null) && (assetMetaData.EntityId == null))
            return;

        if (assetMetaData.Entity == null)
        {
            if (operation == "Update")
                _assetContainer?.RemoveUpdateMessageId(assetMetaData.MessageId);
            else if (operation == "Delete")
                _assetContainer?.RemoveDeleteMessageId(assetMetaData.MessageId);
            else if (operation == "Create")
            {
                if (clientId != null)
                    _testEventContainer.IgnoredParentEntityList.AddItem(clientId, new Asset(Convert.ToInt32(clientId))
                    {
                        Code = null,
                        Description = null
                    });
                
                _assetContainer?.RemoveInsertMessageId(assetMetaData.MessageId);                
            }
        }
        else
        {
            Asset asset =
                (Asset)(JsonConvert.DeserializeObject(assetMetaData.Entity, typeof(Asset)) ?? string.Empty);

            if (asset == null)
                return;

            if (_testEventContainer.CompareTestStarted == false)
                Console.WriteLine($"{operation}: {nameof(Asset)}: {asset.Id}, {asset.Code}, {clientId}");

            if (operation == "Create")
            {
                if (clientId != null)
                {
                    await _assetController.AddAssetAsync(asset);
                    _testEventContainer.CreateQueueManager.Enqueue(clientId, asset);
                }

                if (asset.Id != null)
                {
                    _testEventContainer.AddEntity(EntityType.Asset, asset.Id.Value);
                    _testEventContainer.AssetIdsForTasks.TryAdd(asset.Guid, asset.Id.Value);
                }

                _testEventContainer.StartAssetTasksEvent.Set();
            }
            else if (operation == "Update")
            {
                if (clientId != null)
                {
                    await _assetController.UpdateAssetAsync(asset);
                    int dbVersion = await _assetController.GetAssetVersion(Convert.ToInt64(asset.Id));

                    if (dbVersion == _finalEntityVersion)
                    {
                        _testEventContainer.UpdateQueueManager.Enqueue(clientId, asset);
                    }
                }
            }
            else if (operation == "Delete")
            {
                if (clientId != null)
                {
                    if (assetMetaData.Entity != null)
                    {
                        asset = (Asset)(JsonConvert.DeserializeObject(assetMetaData.Entity, typeof(Asset)) ??
                                        string.Empty);

                        if (asset.Id != null)
                        {
                            await _assetController.DeleteAssetAsync(asset, assetMetaData.MessageId);
                            _testEventContainer.DeletedParentEntityList.AddItem(clientId, asset);
                            _testEventContainer.RemoveEntityId(EntityType.Asset, asset.Id.Value);
                            RemoveItem(_testEventContainer.AssetIdsForTasks, asset.Guid);
                        }
                    }
                }
            }
        }
    }

    private async Task ProcessMqttAssetTaskMessageAsync(EntityMetaData assetTaskMetaData)
    {
        var operation = assetTaskMetaData.Operation;
        var clientId = assetTaskMetaData.ClientId;

        if ((assetTaskMetaData.Entity == null) && (clientId == null) && (assetTaskMetaData.EntityId == null))
            return;

        if (assetTaskMetaData.Entity == null)
        {
            if (operation == "Update")
                _assetTaskContainer?.RemoveUpdateMessageId(assetTaskMetaData.MessageId);
            else if (operation == "Delete")
                _assetTaskContainer?.RemoveDeleteMessageId(assetTaskMetaData.MessageId);
            else if (operation == "Create")
            {
                if (clientId != null)
                    _testEventContainer.IgnoredChildEntityList.AddItem(clientId, new AssetTask(Convert.ToInt32(clientId))
                    {
                        Code = null,
                        Description = null
                    });
                
                _assetTaskContainer?.RemoveInsertMessageId(assetTaskMetaData.MessageId);
            }
        }
        else
        {
            AssetTask assetTask =
                (AssetTask)(JsonConvert.DeserializeObject(assetTaskMetaData.Entity, typeof(AssetTask)) ?? string.Empty);

            if (assetTask == null)
                return;

            if (_testEventContainer.CompareTestStarted == false)
                Console.WriteLine(
                    $"{operation}: {nameof(AssetTask)}: Id: {assetTask.Id}, AssetId: {assetTask.AssetId}, {assetTask.Code}, {clientId}");

            if (operation == "Create")
            {
                if (clientId != null)
                {
                    await _assetTaskController.AddAssetTaskAsync(assetTask);
                    _testEventContainer.CreateQueueManager.Enqueue(clientId, assetTask);

                    if (assetTask.Id != null)
                    {
                        _testEventContainer.AddEntity(EntityType.AssetTask, assetTask.Id.Value);
                    }
                }
            }
            else if (operation == "Update")
            {
                if (clientId != null)
                {
                    await _assetTaskController.UpdateAssetTaskAsync(assetTask);
                    int dbVersion =
                        await _assetTaskController.GetAssetTaskVersion(Convert.ToInt64(assetTask.Id));

                    if (dbVersion == _finalEntityVersion)
                    {
                        _testEventContainer.UpdateQueueManager.Enqueue(clientId, assetTask);
                    }
                }
            }
            else if (operation == "Delete")
            {
                if (clientId != null)
                {
                    if (assetTaskMetaData.Entity != null)
                    {
                        assetTask = (AssetTask)(JsonConvert.DeserializeObject(assetTaskMetaData.Entity,
                            typeof(AssetTask)) ?? string.Empty);

                        if (assetTask.Id != null)
                        {
                            await _assetTaskController.DeleteAssetTaskAsync(assetTask, assetTaskMetaData.MessageId);
                            _testEventContainer.DeletedChildEntityList.AddItem(clientId, assetTask);
                            _testEventContainer.RemoveEntityId(EntityType.AssetTask, assetTask.Id.Value);
                        }
                    }
                }
            }
        }
    }

    private async Task ProcessMqttMeterMessageAsync(EntityMetaData meterMetaData)
    {
        var operation = meterMetaData.Operation;
        var clientId = meterMetaData.ClientId;

        if ((meterMetaData.Entity == null) && (clientId == null) && (meterMetaData.EntityId == null))
            return;

        if (meterMetaData.Entity == null)
        {
            if (operation == "Update")
                _meterContainer?.RemoveUpdateMessageId(meterMetaData.MessageId);
            else if (operation == "Delete")
                _meterContainer?.RemoveDeleteMessageId(meterMetaData.MessageId);
            else if (operation == "Create")
            {
                if (clientId != null)
                    _testEventContainer.IgnoredParentEntityList.AddItem(clientId, new Meter(Convert.ToInt32(clientId))
                    {
                        Code = null,
                        Description = null
                    });
                
                _meterContainer?.RemoveInsertMessageId(meterMetaData.MessageId);                
            }                
        }
        else
        {
            Meter meter =
                (Meter)(JsonConvert.DeserializeObject(meterMetaData.Entity, typeof(Meter)) ?? string.Empty);

            if (meter == null)
                return;

            if (_testEventContainer.CompareTestStarted == false)
                Console.WriteLine(
                    $"{operation}: {nameof(Meter)}: Id: {meter.Id}, Code: {meter.Code}, clientId: {clientId}");

            if (operation == "Create")
            {
                if (clientId != null)
                {
                    await _meterController.AddMeterAsync(meter);
                    _testEventContainer.CreateQueueManager.Enqueue(clientId, meter);

                    if (meter.Id != null)
                    {
                        _testEventContainer.AddEntity(EntityType.Meter, meter.Id.Value);
                        _testEventContainer.MeterIdsForReadings.TryAdd(meter.Guid, meter.Id.Value);
                        _testEventContainer.MeterReadingsSequencer.Add(meter.Id.Value,
                            new TestEventContainer.SequentialSequenceGenerator(1, _meterReadingsPerMeterCount));
                    }
                }

                _testEventContainer.StartMeterReadingsEvent.Set();
            }
            else if (operation == "Update")
            {
                if (clientId != null)
                {
                    await _meterController.UpdateMeterAsync(meter);
                    int dbVersion = await _meterController.GetMeterVersion(Convert.ToInt64(meter.Id));

                    if (dbVersion == _finalEntityVersion)
                    {
                        _testEventContainer.UpdateQueueManager.Enqueue(clientId, meter);
                    }
                }
            }
            else if (operation == "Delete")
            {
                if (clientId != null)
                {
                    if (meterMetaData.Entity != null)
                    {
                        meter = (Meter)(JsonConvert.DeserializeObject(meterMetaData.Entity, typeof(Meter)) ??
                                        string.Empty);

                        if (meter.Id != null)
                        {
                            await _meterController.DeleteMeterAsync(meter, meterMetaData.MessageId);
                            _testEventContainer.DeletedParentEntityList.AddItem(clientId, meter);
                            _testEventContainer.RemoveEntityId(EntityType.Meter, meter.Id.Value);
                            RemoveItem(_testEventContainer.MeterIdsForReadings, meter.Guid);
                        }
                    }
                }
            }
        }
    }

    private async Task ProcessMqttMeterReadingMessageAsync(EntityMetaData meterReadingMetaData)
    {
        var operation = meterReadingMetaData.Operation;
        var clientId = meterReadingMetaData.ClientId;

        if ((meterReadingMetaData.Entity == null) && (clientId == null) && (meterReadingMetaData.EntityId == null))
            return;

        if (meterReadingMetaData.Entity == null)
        {
            if (operation == "Update")
                _meterReadingContainer?.RemoveUpdateMessageId(meterReadingMetaData.MessageId);
            else if (operation == "Delete")
                _meterReadingContainer?.RemoveDeleteMessageId(meterReadingMetaData.MessageId);
            else if (operation == "Create")
            {
                if (clientId != null)
                    _testEventContainer.IgnoredChildEntityList.AddItem(clientId,
                        new MeterReading(Convert.ToInt32(clientId)));

                _meterReadingContainer?.RemoveInsertMessageId(meterReadingMetaData.MessageId);
            }
        }
        else
        {
            MeterReading meterReading =
                (MeterReading)(JsonConvert.DeserializeObject(meterReadingMetaData.Entity, typeof(MeterReading)) ??
                               string.Empty);

            if (meterReading == null)
                return;

            if (_testEventContainer.CompareTestStarted == false)
                Console.WriteLine(
                    $"{operation}: {nameof(MeterReading)}: Id: {meterReading.Id}, meterId: {meterReading.MeterId}, clientId: {clientId}");

            if (operation == "Create")
            {
                if (clientId != null)
                {
                    await _meterReadingController.AddMeterReadingAsync(meterReading);
                    _testEventContainer.CreateQueueManager.Enqueue(clientId, meterReading);

                    if (meterReading.Id != null)
                    {
                        _testEventContainer.AddEntity(EntityType.MeterReading, meterReading.Id.Value);
                    }
                }
            }
            else if (operation == "Update")
            {
                if (clientId != null)
                {
                    await _meterReadingController.UpdateMeterReadingAsync(meterReading);
                    int dbVersion =
                        await _meterReadingController.GetMeterReadingVersion(Convert.ToInt64(meterReading.Id));

                    if (dbVersion == _finalEntityVersion)
                    {
                        _testEventContainer.UpdateQueueManager.Enqueue(clientId, meterReading);
                    }
                }
            }
            else if (operation == "Delete")
            {
                if (clientId != null)
                {
                    if (meterReadingMetaData.Entity != null)
                    {
                        meterReading =
                            (MeterReading)(JsonConvert.DeserializeObject(meterReadingMetaData.Entity,
                                typeof(MeterReading)) ?? string.Empty);

                        if (meterReading.Id != null)
                        {
                            await _meterReadingController.DeleteMeterReadingAsync(meterReading, meterReadingMetaData.MessageId);
                            _testEventContainer.DeletedChildEntityList.AddItem(clientId, meterReading);
                            _testEventContainer.RemoveEntityId(EntityType.MeterReading, meterReading.Id.Value);
                        }
                    }
                }
            }
        }
    }

    public void RemoveItem(ConcurrentDictionary<Guid, long> dictionary, Guid key)
    {
        dictionary.TryRemove(key, out _);
    }

    private class EntityMetaData
    {
        public string MessageId { get; set; } = null!;
        public string? ClientId { get; set; }
        public Guid? MqttSessionId { get; set; }
        public string? EntityType { get; set; }
        public string? Operation { get; set; }
        public string? Entity { get; set; }
        public long? EntityId { get; set; }
    }
}
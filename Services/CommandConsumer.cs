using MqttTestClient.Controllers;
using MqttTestClient.Services.Web;

namespace MqttTestClient.Services;

public class CommandConsumer
{
    private readonly EntityCrudContainer _entityCrudContainer;
    private readonly WebClientService _webService;
    private readonly TestEventContainer _testEventContainer;
    private readonly ToDoItemController _toDoItemController;
    private readonly AssetController _assetController;    
    private readonly MeterController _meterController;    
    private readonly AssetTaskController _assetTaskController;    
    private readonly MeterReadingController _meterReadingController;    

    public CommandConsumer(WebClientService webService, EntityCrudContainer entityCrudContainer,
        TestEventContainer testEventContainer, ToDoItemController toDoItemController,
        AssetController assetController, MeterController meterController,
        AssetTaskController assetTaskController, MeterReadingController meterReadingController)
    {
        _entityCrudContainer = entityCrudContainer;
        _webService = webService;
        _testEventContainer = testEventContainer;
        _toDoItemController = toDoItemController;
        _assetController = assetController;
        _meterController = meterController;
        _assetTaskController = assetTaskController;
        _meterReadingController = meterReadingController;
    }

    public async Task Run()
    {
        List<Task> tasks = [];
        
        var createTask = RunCreate();
        tasks.Add(createTask);        

        if (_entityCrudContainer.DoUpdate)
        {
            var updateTask = RunUpdate();
            tasks.Add(updateTask);
        }

        if (_entityCrudContainer.DoDelete)
        {
            var deleteTask = RunDelete();
            tasks.Add(deleteTask);
        }

        await Task.WhenAll(tasks);
        Console.WriteLine("Consumers done...");
    }

    private async Task RunCreate()
    {
        await Task.Run(async () =>
        {
            while (true)
            {
                while (_entityCrudContainer.CreateEntityQueue.Count > 0)
                {
                    if (!_entityCrudContainer.CreateEntityQueue.TryDequeue(out var operation))
                            continue; 

                    if (operation.CrudOperation == CrudOperation.Create)
                    {
                        if (operation.EntityType == EntityType.ToDoItem)
                            await _webService.ProcessItemPostAsync(operation.MqttSessionId, operation.ClientId);

                        else if (operation.EntityType == EntityType.Asset)
                            await _webService.ProcessAssetPostAsync(operation.MqttSessionId, operation);

                        else if (operation.EntityType == EntityType.Meter)
                            await _webService.ProcessMeterPostAsync(operation.MqttSessionId, operation);

                        else if (operation.EntityType == EntityType.AssetTask)
                            await _webService.ProcessAssetTaskPostAsync(operation.MqttSessionId, operation);

                        else if (operation.EntityType == EntityType.MeterReading)
                            await _webService.ProcessMeterReadingPostAsync(operation.MqttSessionId, operation);
                    }
                }

                foreach (var container in _testEventContainer.EntityContainers)
                {
                    if (container.Value != null) container.Value.HasInsertSendCompleted = true;
                }

                return;
            }
        });
    }
    
    private async Task RunUpdate()
    {
        await Task.Run(async () =>
        {
            _testEventContainer.AllInsertsCompletedEvent.WaitOne();
            
            while (true)
            {
                while (_entityCrudContainer.UpdateEntityQueue.Count > 0)
                {
                    if (!_entityCrudContainer.UpdateEntityQueue.TryDequeue(out var operation))
                        continue; 

                    if (operation.CrudOperation == CrudOperation.Update)
                    {
                        if (operation.EntityType == EntityType.ToDoItem)
                        {
                            long? itemId = _testEventContainer.GetEntityId(EntityType.ToDoItem);
                
                            if (itemId == null)
                                continue;

                            var localTodoItem = await _toDoItemController.GetItemById(itemId.Value);
                            
                            if (localTodoItem == null)
                                continue;
                            
                            var messageId = await _webService.ProcessItemPutAsync(operation.MqttSessionId, localTodoItem);
                            _toDoItemController.EntityContainer?.RemoveUpdateMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.Asset)
                        {
                            long? assetId = _testEventContainer.GetEntityId(EntityType.Asset);
                
                            if (assetId == null)
                                continue;

                            var localAsset = await _assetController.GetAssetByIdAsync(assetId.Value);
                            
                            if (localAsset == null)
                                continue;
                            
                            var messageId = await _webService.ProcessAssetPutAsync(operation.MqttSessionId, localAsset);
                            _assetController.EntityContainer?.RemoveUpdateMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.Meter)
                        {
                            long? meterId = _testEventContainer.GetEntityId(EntityType.Meter);
                
                            if (meterId == null)
                                continue;

                            var localMeter = await _meterController.GetMeter(meterId.Value);
                            
                            if (localMeter == null)
                                continue;
                            
                            var messageId = await _webService.ProcessMeterPutAsync(operation.MqttSessionId, localMeter);
                            _meterController.EntityContainer?.RemoveUpdateMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.AssetTask)
                        {
                            long? assetTaskId = _testEventContainer.GetEntityId(EntityType.AssetTask);
                
                            if (assetTaskId == null)
                                continue;

                            var localAssetTask = await _assetTaskController.GetAssetTask(assetTaskId.Value);
                            
                            if (localAssetTask == null)
                                continue;
                            
                            var messageId = await _webService.ProcessAssetTaskPutAsync(operation.MqttSessionId, localAssetTask);
                            _assetTaskController.EntityContainer?.RemoveUpdateMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.MeterReading)
                        {
                            long? meterReadingId = _testEventContainer.GetEntityId(EntityType.MeterReading);
                
                            if (meterReadingId == null)
                                continue;

                            var localMeterReading = await _meterReadingController.GetLocalMeterReadingByIdAsync(meterReadingId.Value);
                            
                            if (localMeterReading == null)
                                continue;
                            
                            var messageId = await _webService.ProcessMeterReadingPutAsync(operation.MqttSessionId, localMeterReading);
                            _meterReadingController.EntityContainer?.RemoveUpdateMessageId(messageId);
                        }
                    }
                }

                foreach (var container in _testEventContainer.EntityContainers)
                {
                    if (container.Value != null) container.Value.HasUpdateSendCompleted = true;
                }
                
                return;
            }
        });
    }    
    
    private async Task RunDelete()
    {
        await Task.Run(async () =>
        {
            _testEventContainer.AllInsertsCompletedEvent.WaitOne();
            
            while (true)
            {
                while (_entityCrudContainer.DeleteEntityQueue.Count > 0)
                {
                    if (!_entityCrudContainer.DeleteEntityQueue.TryDequeue(out var operation))
                        continue; 

                    if (operation.CrudOperation == CrudOperation.Delete)
                    {
                        if (operation.EntityType == EntityType.ToDoItem)
                        {
                            long? itemId = _testEventContainer.GetEntityId(EntityType.ToDoItem);
                
                            if (itemId == null)
                                continue;

                            var messageId = await _webService.ProcessItemDeleteAsync(operation.MqttSessionId, itemId.Value);
                            _toDoItemController.EntityContainer?.RemoveDeleteMessageId(messageId);
                        }

                        else if (operation.EntityType == EntityType.Asset)
                        {
                            long? assetId = _testEventContainer.GetEntityId(EntityType.Asset);
                
                            if (assetId == null)
                                continue;

                            var messageId = await _webService.ProcessAssetDeleteAsync(operation.MqttSessionId, assetId.Value);
                            _assetController.EntityContainer?.RemoveDeleteMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.Meter)
                        {
                            long? meterId = _testEventContainer.GetEntityId(EntityType.Meter);
                
                            if (meterId == null)
                                continue;

                            var messageId = await _webService.ProcessMeterDeleteAsync(operation.MqttSessionId, meterId.Value);
                            _testEventContainer.EntityContainers[EntityType.Meter]?.RemoveDeleteMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.AssetTask)
                        {
                            long? assetTaskId = _testEventContainer.GetEntityId(EntityType.AssetTask);
                
                            if (assetTaskId == null)
                                continue;

                            var messageId = await _webService.ProcessAssetTaskDeleteAsync(operation.MqttSessionId, assetTaskId.Value);
                            _assetTaskController.EntityContainer?.RemoveDeleteMessageId(messageId);
                        }
                        else if (operation.EntityType == EntityType.MeterReading)
                        {
                            long? meterReadingId = _testEventContainer.GetEntityId(EntityType.MeterReading);
                
                            if (meterReadingId == null)
                                continue;

                            var messageId = await _webService.ProcessMeterReadingDeleteAsync(operation.MqttSessionId, meterReadingId.Value);
                            _meterReadingController.EntityContainer?.RemoveDeleteMessageId(messageId);
                        }
                    }
                }

                foreach (var container in _testEventContainer.EntityContainers)
                {
                    if (container.Value != null) container.Value.HasDeleteSendCompleted = true;
                }
                
                return;
            }
        });
    }    
}
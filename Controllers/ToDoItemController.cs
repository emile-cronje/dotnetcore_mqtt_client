using MqttTestClient.DataAccess;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.Controllers;

public class ToDoItemController : EntityController
{
    private readonly IToDoItemDao _dao;
    private List<(ToDoItem item, string messageId)> _currentDeleteBatch = [];
    private List<ToDoItem> _currentInsertBatch = [];
    private List<ToDoItem> _currentUpdateBatch = [];

    public ToDoItemController(IToDoItemDao dao, EntityContainer toDoEntityContainer,
        TestEventContainer testEventContainer) : base(toDoEntityContainer, testEventContainer)
    {
        _dao = dao;
        FlushTimer = new Timer(FlushTimerCallback, null, FlushTimeOut, FlushTimeOut);
    }

    public async Task AddItemAsync(ToDoItem item)
    {
        await InsertBatchLockSem.WaitAsync();

        try
        {
            if (_currentInsertBatch.Count == 0)
                InsertBatchStartTime = DateTime.UtcNow;

            _currentInsertBatch.Add(item);

            if (_currentInsertBatch.Count >= BatchSize)
                await FlushInsertBatch(true);
        }
        finally
        {
            InsertBatchLockSem.Release();
        }
    }

    public async Task UpdateItemAsync(ToDoItem item)
    {
        await UpdateBatchLockSem.WaitAsync();

        try
        {
            if (_currentUpdateBatch.Count == 0)
                UpdateBatchStartTime = DateTime.UtcNow;

            _currentUpdateBatch.Add(item);

            if (_currentUpdateBatch.Count >= BatchSize)
                await FlushUpdateBatch(true);
        }
        finally
        {
            UpdateBatchLockSem.Release();
        }
    }

    public async Task DeleteItemAsync(ToDoItem item, string messageId)
    {
        await DeleteBatchLockSem.WaitAsync();

        try
        {
            if (_currentDeleteBatch.Count == 0)
                DeleteBatchStartTime = DateTime.UtcNow;

            _currentDeleteBatch.Add((item, messageId));

            if (_currentDeleteBatch.Count >= BatchSize)
                await FlushDeleteBatch(true);
        }
        finally
        {
            DeleteBatchLockSem.Release();
        }
    }

    private async Task FlushInsertBatch(bool isFullBatch)
    {
        await InsertFlushBatchLockSem.WaitAsync();

        try
        {
            if (!isFullBatch && DateTime.UtcNow - InsertBatchStartTime < FlushTimeOut)
                return;

            var batchToFlush = _currentInsertBatch;
            _currentInsertBatch = [];

            await _dao.BatchInsert(batchToFlush);

            EntityContainer?.RemoveInsertMessageIds(batchToFlush.Select(x => x.MessageId).ToList());
            TestEventContainer.StartCompareMonitorEvent.Set();
        }
        finally
        {
            InsertFlushBatchLockSem.Release();
        }
    }

    private async Task FlushUpdateBatch(bool isFullBatch)
    {
        await UpdateFlushBatchLockSem.WaitAsync();

        try
        {
            if (!isFullBatch && DateTime.UtcNow - UpdateBatchStartTime < FlushTimeOut)
                return;

            var batchToFlush = _currentUpdateBatch;
            _currentUpdateBatch = [];

            await _dao.BatchUpdate(batchToFlush, BatchSize);
            EntityContainer?.RemoveUpdateMessageIds(batchToFlush.Select(x => x.MessageId).ToList());            
        }
        finally
        {
            UpdateFlushBatchLockSem.Release();
        }
    }

    private async Task FlushDeleteBatch(bool isFullBatch)
    {
        await DeleteFlushBatchLockSem.WaitAsync();

        try
        {
            if (!isFullBatch && DateTime.UtcNow - DeleteBatchStartTime < FlushTimeOut)
                return;

            var batchToFlush = _currentDeleteBatch;
            _currentDeleteBatch = [];

            await _dao.BatchDelete(batchToFlush.Where(x => x.item.Id.HasValue).Select(x => x.item.Id!.Value));
        }
        finally
        {
            DeleteFlushBatchLockSem.Release();
        }
    }

    public async Task<ToDoItem?> GetItemById(long itemId)
    {
        return await _dao.GetItem(itemId);
    }

    public async Task<int> GetItemVersion(long itemId)
    {
        return await _dao.GetEntityVersion(itemId);
    }

    public async Task<int> GetLocalItemCount()
    {
        return await _dao.GetEntityCount();
    }

    private void FlushTimerCallback(object? state)
    {
        _ = Task.Run(FlushTimerCallbackAsync);
    }

    private async Task FlushTimerCallbackAsync()
    {
        if (_currentInsertBatch.Count > 0)
            await FlushInsertBatch(false);

        if (_currentUpdateBatch.Count > 0)
            await FlushUpdateBatch(false);

        if (_currentDeleteBatch.Count > 0)
            await FlushDeleteBatch(false);
    }
}
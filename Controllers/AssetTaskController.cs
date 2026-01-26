using MqttTestClient.DataAccess;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.Controllers;

public class AssetTaskController : EntityController
{
    private readonly IAssetTaskDao _dao;
    private List<(AssetTask assetTask, string messageId)> _currentDeleteBatch = [];
    private List<AssetTask> _currentInsertBatch = [];
    private List<AssetTask> _currentUpdateBatch = [];

    public AssetTaskController(IAssetTaskDao dao,
        EntityContainer assetTaskEntityContainer,
        TestEventContainer testEventContainer) : base(assetTaskEntityContainer, testEventContainer)
    {
        _dao = dao;
        FlushTimer = new Timer(FlushTimerCallback, null, FlushTimeOut, FlushTimeOut);
    }

    public async Task AddAssetTaskAsync(AssetTask assetTask)
    {
        await InsertBatchLockSem.WaitAsync();

        try
        {
            if (_currentInsertBatch.Count == 0)
                InsertBatchStartTime = DateTime.UtcNow;

            _currentInsertBatch.Add(assetTask);

            if (_currentInsertBatch.Count >= BatchSize)
                await FlushInsertBatch(true);
        }
        finally
        {
            InsertBatchLockSem.Release();
        }
    }

    public async Task UpdateAssetTaskAsync(AssetTask assetTask)
    {
        await UpdateBatchLockSem.WaitAsync();

        try
        {
            if (_currentUpdateBatch.Count == 0)
                UpdateBatchStartTime = DateTime.UtcNow;

            _currentUpdateBatch.Add(assetTask);

            if (_currentUpdateBatch.Count >= BatchSize)
                await FlushUpdateBatch(true);
        }
        finally
        {
            UpdateBatchLockSem.Release();
        }
    }

    public async Task DeleteAssetTaskAsync(AssetTask assetTask, string messageId)
    {
        await DeleteBatchLockSem.WaitAsync();

        try
        {
            if (_currentDeleteBatch.Count == 0)
                DeleteBatchStartTime = DateTime.UtcNow;

            _currentDeleteBatch.Add((assetTask, messageId));

            if (_currentDeleteBatch.Count >= BatchSize)
                await FlushDeleteBatch(true);
        }
        finally
        {
            DeleteBatchLockSem.Release();
        }
    }

    public async Task<AssetTask?> GetAssetTask(long assetTaskId)
    {
        return await _dao.GetAssetTask(assetTaskId);
    }

    public async Task<int> GetAssetTaskVersion(long assetTaskId)
    {
        return await _dao.GetEntityVersion(assetTaskId);
    }

    public async Task<int> GetLocalAssetTaskCount()
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

            await _dao.BatchDelete(batchToFlush.Where(x => x.assetTask.Id.HasValue).Select(x => x.assetTask.Id!.Value));
        }
        finally
        {
            DeleteFlushBatchLockSem.Release();
        }
    }
}
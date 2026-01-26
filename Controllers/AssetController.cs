using MqttTestClient.DataAccess;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.Controllers;

public class AssetController : EntityController
{
    private readonly IAssetDao _dao;
    private List<(Asset asset, string messageId)> _currentDeleteBatch = [];
    private List<Asset> _currentInsertBatch = [];
    private List<Asset> _currentUpdateBatch = [];

    public AssetController(IAssetDao dao, EntityContainer assetEntityContainer,
        TestEventContainer testEventContainer) : base(assetEntityContainer, testEventContainer)
    {
        _dao = dao;
        FlushTimer = new Timer(FlushTimerCallback, null, FlushTimeOut, FlushTimeOut);
    }

    public async Task AddAssetAsync(Asset asset)
    {
        await InsertBatchLockSem.WaitAsync();

        try
        {
            if (_currentInsertBatch.Count == 0)
                InsertBatchStartTime = DateTime.UtcNow;

            _currentInsertBatch.Add(asset);

            if (_currentInsertBatch.Count >= BatchSize)
                await FlushInsertBatch(true);
        }
        finally
        {
            InsertBatchLockSem.Release();
        }
    }

    public async Task UpdateAssetAsync(Asset asset)
    {
        await UpdateBatchLockSem.WaitAsync();

        try
        {
            if (_currentUpdateBatch.Count == 0)
                UpdateBatchStartTime = DateTime.UtcNow;

            _currentUpdateBatch.Add(asset);

            if (_currentUpdateBatch.Count >= BatchSize)
                await FlushUpdateBatch(true);
        }
        finally
        {
            UpdateBatchLockSem.Release();
        }
    }

    public async Task<Asset?> GetAssetByIdAsync(long assetId)
    {
        return await _dao.GetAsset(assetId);
    }

    public async Task DeleteAssetAsync(Asset asset, string messageId)
    {
        await DeleteBatchLockSem.WaitAsync();

        try
        {
            if (_currentDeleteBatch.Count == 0)
                DeleteBatchStartTime = DateTime.UtcNow;

            _currentDeleteBatch.Add((asset, messageId));

            if (_currentDeleteBatch.Count >= BatchSize)
                await FlushDeleteBatch(true);
        }
        finally
        {
            DeleteBatchLockSem.Release();
        }
    }

    public async Task<int> GetAssetVersion(long assetId)
    {
        return await _dao.GetEntityVersion(assetId);
    }

    public async Task<int> GetLocalAssetCount()
    {
        return await _dao.GetEntityCount();
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

    private void FlushTimerCallback(object? state)
    {
        _ = Task.Run(FlushTimerCallbackAsync);
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

            await _dao.BatchDelete(batchToFlush.Where(x => x.asset.Id.HasValue).Select(x => x.asset.Id!.Value));
        }
        finally
        {
            DeleteFlushBatchLockSem.Release();
        }
    }
}
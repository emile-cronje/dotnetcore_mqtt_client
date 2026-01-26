using MqttTestClient.DataAccess;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.Controllers;

public class MeterReadingController : EntityController
{
    private readonly IMeterReadingDao _dao;
    private List<(MeterReading meterReading, string messageId)> _currentDeleteBatch = [];
    private List<MeterReading> _currentInsertBatch = [];
    private List<MeterReading> _currentUpdateBatch = [];

    public MeterReadingController(IMeterReadingDao dao,
        EntityContainer meterReadingEntityContainer,
        TestEventContainer testEventContainer) : base(meterReadingEntityContainer, testEventContainer)
    {
        _dao = dao;
        FlushTimer = new Timer(FlushTimerCallback, null, FlushTimeOut, FlushTimeOut);
    }

    public async Task AddMeterReadingAsync(MeterReading meterReading)
    {
        await InsertBatchLockSem.WaitAsync();

        try
        {
            if (_currentInsertBatch.Count == 0)
                InsertBatchStartTime = DateTime.UtcNow;

            _currentInsertBatch.Add(meterReading);

            if (_currentInsertBatch.Count >= BatchSize)
                await FlushInsertBatch(true);
        }
        finally
        {
            InsertBatchLockSem.Release();
        }
    }

    public async Task UpdateMeterReadingAsync(MeterReading meterReading)
    {
        await UpdateBatchLockSem.WaitAsync();

        try
        {
            if (_currentUpdateBatch.Count == 0)
                UpdateBatchStartTime = DateTime.UtcNow;

            _currentUpdateBatch.Add(meterReading);

            if (_currentUpdateBatch.Count >= BatchSize)
                await FlushUpdateBatch(true);
        }
        finally
        {
            UpdateBatchLockSem.Release();
        }
    }

    public async Task DeleteMeterReadingAsync(MeterReading meterReading, string messageId)
    {
        await DeleteBatchLockSem.WaitAsync();

        try
        {
            if (_currentDeleteBatch.Count == 0)
                DeleteBatchStartTime = DateTime.UtcNow;

            _currentDeleteBatch.Add((meterReading, messageId));

            if (_currentDeleteBatch.Count >= BatchSize)
                await FlushDeleteBatch(true);
        }
        finally
        {
            DeleteBatchLockSem.Release();
        }
    }

    public async Task<MeterReading?> GetLocalMeterReadingByIdAsync(long meterReadingId)
    {
        return await _dao.GetMeterReading(meterReadingId);
    }

    public async Task<int> GetMeterReadingVersion(long meterReadingId)
    {
        return await _dao.GetEntityVersion(meterReadingId);
    }

    public async Task<int> GetLocalMeterReadingCountAsync()
    {
        return await _dao.GetEntityCount();
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
            
            await _dao.BatchDelete(batchToFlush.Where(x => x.meterReading.Id.HasValue)
                .Select(x => x.meterReading.Id!.Value));
        }
        finally
        {
            DeleteFlushBatchLockSem.Release();
        }
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
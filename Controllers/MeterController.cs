using MqttTestClient.DataAccess;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.Controllers;

public class MeterController : EntityController
{
    private readonly IMeterDao _dao;
    private readonly IMeterReadingDao _meterReadingDao;
    private List<(Meter meter, string messageId)> _currentDeleteBatch = [];
    private List<Meter> _currentInsertBatch = [];
    private List<Meter> _currentUpdateBatch = [];

    public MeterController(IMeterDao dao,
        IMeterReadingDao meterReadingDao, EntityContainer meterEntityContainer,
        TestEventContainer testEventContainer) : base(meterEntityContainer, testEventContainer)
    {
        _dao = dao;
        _meterReadingDao = meterReadingDao;
        FlushTimer = new Timer(FlushTimerCallback!, null, FlushTimeOut, FlushTimeOut);
    }

    public async Task AddMeterAsync(Meter meter)
    {
        await InsertBatchLockSem.WaitAsync();

        try
        {
            if (_currentInsertBatch.Count == 0)
                InsertBatchStartTime = DateTime.UtcNow;

            _currentInsertBatch.Add(meter);

            if (_currentInsertBatch.Count >= BatchSize)
                await FlushInsertBatch(true);
        }
        finally
        {
            InsertBatchLockSem.Release();
        }
    }

    public async Task UpdateMeterAsync(Meter meter)
    {
        await UpdateBatchLockSem.WaitAsync();

        try
        {
            if (_currentUpdateBatch.Count == 0)
                UpdateBatchStartTime = DateTime.UtcNow;

            _currentUpdateBatch.Add(meter);

            if (_currentUpdateBatch.Count >= BatchSize)
                await FlushUpdateBatch(true);
        }
        finally
        {
            UpdateBatchLockSem.Release();
        }
    }

    public async Task DeleteMeterAsync(Meter meter, string messageId)
    {
        await DeleteBatchLockSem.WaitAsync();

        try
        {
            if (_currentDeleteBatch.Count == 0)
                DeleteBatchStartTime = DateTime.UtcNow;

            _currentDeleteBatch.Add((meter, messageId));

            if (_currentDeleteBatch.Count >= BatchSize)
                await FlushDeleteBatch(true);
        }
        finally
        {
            DeleteBatchLockSem.Release();
        }
    }

    public async Task<Meter?> GetMeter(long meterId)
    {
        return await _dao.GetMeter(meterId);
    }

    public async Task<int> GetMeterVersion(long meterId)
    {
        return await _dao.GetEntityVersion(meterId);
    }

    public async Task<decimal?> GetMeterAdrAsync(long meterId)
    {
        //var adr = await CalculateAverageDailyRate(meterId);//await _dao.GetMeterAdr(meterId));
        var adr = await _dao.GetMeterAdr(meterId);

        return adr;
    }

    public async Task<int> GetLocalMeterCountAsync()
    {
        return await _dao.GetEntityCount();
    }

    public async Task<decimal?> CalculateAverageDailyRate(long meterId)
    {
        var readings = await _meterReadingDao.FetchMeterReadingsForMeter(meterId);

        if (readings.Count < 2) return 0;
        //throw new ArgumentException("At least two readings are required to calculate the rate.");
        var adr = CalculateAverageDailyRate2(readings);
        return adr;
    }

    private decimal? CalculateAverageDailyRate2(List<MeterReading> meterReadings)
    {
        if (meterReadings.Count < 2)
        {
            Console.WriteLine("Too few readings...");
            return 0;
        }

        decimal totalRate = 0;
        var count = 0;

        for (var i = 1; i < meterReadings.Count; i++)
        {
            var previous = meterReadings[i - 1];
            var current = meterReadings[i];

            var deltaReading = (decimal)(current.Reading - previous.Reading)!;
            if (current.ReadingOn != null)
                if (previous.ReadingOn != null)
                {
                    var deltaDays = (current.ReadingOn.Value - previous.ReadingOn.Value).TotalDays;

                    if (deltaDays > 0)
                    {
                        var dailyRate = deltaReading / (decimal)deltaDays;
                        totalRate += dailyRate;
                        count++;
                    }
                }
        }

        return count > 0 ? totalRate / count : 0;
    }

    private void FlushTimerCallback(object state)
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

            await _dao.BatchDelete(batchToFlush.Where(x => x.meter.Id!.HasValue)
                .Select(x => x.meter.Id!.Value));
            EntityContainer?.RemoveDeleteMessageIds(batchToFlush.Select(x => x.messageId).ToList());
        }
        finally
        {
            DeleteFlushBatchLockSem.Release();
        }
    }
}
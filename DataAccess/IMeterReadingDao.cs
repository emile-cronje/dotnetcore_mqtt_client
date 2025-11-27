using MqttTestClient.Models;

namespace MqttTestClient.DataAccess;

public interface IMeterReadingDao : IPgDao
{
    void Initialise();
    Task<MeterReading?> GetMeterReading(long meterReadingId);
    Task<List<MeterReading>> FetchMeterReadingsForMeter(long meterId);
    Task BatchInsert(List<MeterReading> meterReadings);
    Task BatchUpdate(List<MeterReading> meterReadings, int batchSize);
    Task BatchDelete(IEnumerable<long> ids);            
}
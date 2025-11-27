using MqttTestClient.Models;

namespace MqttTestClient.DataAccess;

public interface IMeterDao : IPgDao
{
    void Initialise();
    Task<Meter?> GetMeter(long meterId);
    Task<decimal?> GetMeterAdr(long meterId);
    Task BatchInsert(List<Meter> meters);
    Task BatchUpdate(List<Meter> meters, int batchSize);
    Task BatchDelete(IEnumerable<long> ids);        
}
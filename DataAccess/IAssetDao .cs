using MqttTestClient.Models;

namespace MqttTestClient.DataAccess;

public interface IAssetDao : IPgDao
{
    void Initialise();
    Task<Asset?> GetAsset(long assetId);
    Task BatchInsert(List<Asset> items);
    Task BatchUpdate(List<Asset> assets, int batchSize);
    Task BatchDelete(IEnumerable<long> ids);    
}
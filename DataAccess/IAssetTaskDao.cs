using MqttTestClient.Models;

namespace MqttTestClient.DataAccess;

public interface IAssetTaskDao : IPgDao
{
    void Initialise();
    Task<AssetTask?> GetAssetTask(long assetTaskId);
    Task BatchInsert(List<AssetTask> assetTasks);
    Task BatchUpdate(List<AssetTask> assetTasks, int batchSize);
    Task BatchDelete(IEnumerable<long> ids);        
}
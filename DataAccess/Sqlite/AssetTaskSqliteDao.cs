using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class AssetTaskSqliteDao : SqliteDao, IAssetTaskDao
{
    private readonly string _tableName;

    public AssetTaskSqliteDao(IDbConnectionPool connectionPool) : base(connectionPool)
    {
        _tableName = "asset_tasks";
    }

    public async Task<int> AddAssetTask(string clientId, AssetTask assetTask)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (CLIENT_ID, REMOTE_ID, VERSION, CODE, DESCRIPTION, IS_RFS) VALUES ('{clientId}', '{assetTask.Id}', {assetTask.Version}, '{assetTask.Code}', '{assetTask.Description}', {assetTask.IsRfs})",
            connection);

        int rowsAffected;

        try
        {
            rowsAffected = await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<int> UpdateAssetTask(string clientId, AssetTask assetTask)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = {assetTask.Version}, CODE = '{assetTask.Code}', DESCRIPTION = '{assetTask.Description}', IS_RFS = {assetTask.IsRfs} WHERE REMOTE_ID = '{assetTask.Id}' AND CLIENT_ID = '{clientId}'",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<AssetTask?> GetAssetTask(long assetTaskId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"SELECT remote_id, version, code, description, is_rfs, client_id FROM {_tableName} WHERE remote_id = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", assetTaskId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localAssetTask = new AssetTask(reader.GetInt32(5))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            Code = reader.GetString(2),
            Description = reader.GetString(3),
            IsRfs = reader.GetBoolean(4)
        };

        return localAssetTask;
    }

    public async Task<int> GetAssetTaskVersion(string clientId, long assetTaskId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT version from {_tableName} where remote_id = '{assetTaskId}' and client_id = '{clientId}'",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.Read()) dbVersion = reader.GetInt32(0);
        ReturnConnection(connection);

        return dbVersion;
    }

    public async Task<int> GetAssetTaskCountForClient(string clientId)
    {
        var itemCount = 0;
        var connection = GetConnection();

        await using var cmdCount =
            new SqliteCommand($"SELECT count(*) from {_tableName} where client_id = '{clientId}'", connection);
        await using var readerCount = await cmdCount.ExecuteReaderAsync();

        if (readerCount.Read())
            itemCount = readerCount.GetInt32(0);

        ReturnConnection(connection);
        return itemCount;
    }

    public async Task<int> GetAssetTaskCount()
    {
        var itemCount = 0;
        var connection = GetConnection();

        await using var cmdCount =
            new SqliteCommand($"SELECT count(*) from {_tableName}", connection);
        await using var readerCount = await cmdCount.ExecuteReaderAsync();

        if (readerCount.Read())
            itemCount = readerCount.GetInt32(0);

        ReturnConnection(connection);
        return itemCount;
    }

    public async Task<int> DeleteAssetTask(string clientId, long assetId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE REMOTE_ID = '{assetId}' AND CLIENT_ID = '{clientId}'",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();

        return rowsAffected;
    }

    public void Initialise()
    {
        var connection = GetConnection();

        var command = $"DROP TABLE IF EXISTS {_tableName}";
        var cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        command =
            $"CREATE TABLE {_tableName} (CLIENT_ID TEXT, REMOTE_ID TEXT, VERSION INTEGER, CODE TEXT, DESCRIPTION TEXT, IS_RFS INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        ReturnConnection(connection);
    }

    public Task BatchInsert(List<AssetTask> assetTasks)
    {
        throw new NotImplementedException();
    }

    public Task BatchUpdate(List<AssetTask> assetTasks, int batchSize)
    {
        throw new NotImplementedException();
    }

    public Task BatchDelete(IEnumerable<long> ids)
    {
        throw new NotImplementedException();
    }

    public Task<int> GetEntityVersion(long entityId)
    {
        throw new NotImplementedException();
    }

    public Task<int> GetEntityCount()
    {
        throw new NotImplementedException();
    }
}
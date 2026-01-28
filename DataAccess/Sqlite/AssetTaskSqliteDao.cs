using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class AssetTaskSqliteDao : SqliteDao, IAssetTaskDao
{
    private readonly string _tableName;

    public AssetTaskSqliteDao(IDbConnectionPool connectionPool, string tableName = "asset_tasks") : base(connectionPool)
    {
        _tableName = tableName;
    }

    public async Task<int> AddAssetTask(string clientId, AssetTask assetTask)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (ID, VERSION, CLIENT_ID, MESSAGE_ID, CODE, DESCRIPTION, IS_RFS) VALUES ({assetTask.Id}, {assetTask.Version}, {clientId}, '{assetTask.MessageId}', '{assetTask.Code}', '{assetTask.Description}', {assetTask.IsRfs})",
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
            $"UPDATE {_tableName} SET VERSION = {assetTask.Version}, MESSAGE_ID = '{assetTask.MessageId}', CODE = '{assetTask.Code}', DESCRIPTION = '{assetTask.Description}', IS_RFS = {assetTask.IsRfs} WHERE ID = {assetTask.Id} AND CLIENT_ID = {clientId}",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<AssetTask?> GetAssetTask(long assetTaskId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, CODE, DESCRIPTION, IS_RFS, CLIENT_ID FROM {_tableName} WHERE ID = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", assetTaskId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localAssetTask = new AssetTask(reader.GetInt32(6))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MessageId = reader.GetString(2),
            Code = reader.GetString(3),
            Description = reader.GetString(4),
            IsRfs = reader.GetBoolean(5)
        };

        return localAssetTask;
    }

    public async Task<int> GetAssetTaskVersion(string clientId, long assetTaskId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {assetTaskId} AND CLIENT_ID = {clientId}",
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
            new SqliteCommand($"SELECT COUNT(*) FROM {_tableName} WHERE CLIENT_ID = {clientId}", connection);
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
            new SqliteCommand($"SELECT COUNT(*) FROM {_tableName}", connection);
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
            $"DELETE FROM {_tableName} WHERE ID = {assetId} AND CLIENT_ID = {clientId}",
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
            $"CREATE TABLE {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, CODE TEXT, DESCRIPTION TEXT, IS_RFS INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        command = $"CREATE UNIQUE INDEX idx_{_tableName}_message_id ON {_tableName}(MESSAGE_ID)";
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

    public async Task<int> GetEntityVersion(long entityId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {entityId}",
            connection);

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!reader.HasRows)
            {
                ReturnConnection(connection);
                return -1;
            }

            reader.Read();
            var version = reader.GetInt32(0);
            ReturnConnection(connection);
            return version;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    public async Task<int> GetEntityCount()
    {
        var connection = GetConnection();

        var cmd = new SqliteCommand($"SELECT COUNT(*) FROM {_tableName}", connection);

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            var count = reader.GetInt32(0);
            ReturnConnection(connection);
            return count;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}
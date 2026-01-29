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

    public async Task<int> UpdateAssetTaskOld(string clientId, AssetTask assetTask)
    {
        var connection = GetConnection();

        // Fetch the current version from the database
        await using var versionCmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = @ID AND CLIENT_ID = {clientId}",
            connection);
        versionCmd.Parameters.AddWithValue("@ID", assetTask.Id);
        
        await using var versionReader = await versionCmd.ExecuteReaderAsync();
        if (!versionReader.Read())
        {
            ReturnConnection(connection);
            throw new InvalidOperationException($"AssetTask ID {assetTask.Id} not found");
        }
        
        var currentVersion = versionReader.GetInt32(0);

        // Now update using the current version from the database
        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = VERSION + 1, MESSAGE_ID = @MESSAGE_ID, CODE = @CODE, DESCRIPTION = @DESCRIPTION, IS_RFS = @IS_RFS WHERE ID = @ID AND CLIENT_ID = {clientId} AND VERSION = @VERSION",
            connection);
        cmd.Parameters.AddWithValue("@ID", assetTask.Id);
        cmd.Parameters.AddWithValue("@VERSION", currentVersion);
        cmd.Parameters.AddWithValue("@MESSAGE_ID", assetTask.MessageId);
        cmd.Parameters.AddWithValue("@CODE", assetTask.Code);
        cmd.Parameters.AddWithValue("@DESCRIPTION", assetTask.Description);
        cmd.Parameters.AddWithValue("@IS_RFS", assetTask.IsRfs);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);
        
        if (rowsAffected == 0)
        {
            throw new InvalidOperationException($"Version mismatch for AssetTask ID {assetTask.Id}. Current DB version {currentVersion}, in-memory version {assetTask.Version}");
        }

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
        
        InitializeWalMode(connection);

        ReturnConnection(connection);
    }

    public async Task BatchInsert(List<AssetTask> assetTasks)
    {
        foreach (var assetTask in assetTasks) await AddAssetTask(assetTask.ClientId.ToString(), assetTask);
    }

    public async Task BatchUpdate(List<AssetTask> assetTasks, int batchSize)
    {
        foreach (var assetTask in assetTasks) await UpdateAssetTask(assetTask.ClientId.ToString(), assetTask);
    }

    public async Task BatchDelete(IEnumerable<long> ids)
    {
        foreach (var id in ids)
        {
            var connection = GetConnection();
            await using var cmd = new SqliteCommand(
                $"DELETE FROM {_tableName} WHERE ID = {id}",
                connection);
            await cmd.ExecuteNonQueryAsync();
            ReturnConnection(connection);
        }
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
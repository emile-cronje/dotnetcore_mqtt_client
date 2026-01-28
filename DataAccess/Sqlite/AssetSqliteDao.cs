using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class AssetSqliteDao : SqliteDao, IAssetDao
{
    private readonly string _tableName;

    public AssetSqliteDao(IDbConnectionPool connectionPool, string tableName = "asset") : base(connectionPool)
    {
        _tableName = tableName;
    }

    public async Task<int> AddAsset(string clientId, Asset asset)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (ID, VERSION, CLIENT_ID, MESSAGE_ID, CODE, DESCRIPTION, IS_MSI) VALUES ({asset.Id}, {asset.Version}, {clientId}, '{asset.MessageId}', '{asset.Code}', '{asset.Description}', {asset.IsMsi})",
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

    public async Task<int> UpdateAsset(string clientId, Asset asset)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = {asset.Version}, MESSAGE_ID = '{asset.MessageId}', CODE = '{asset.Code}', DESCRIPTION = '{asset.Description}', IS_MSI = {asset.IsMsi} WHERE CLIENT_ID = {clientId} AND ID = {asset.Id}",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);

        return rowsAffected;
    }

    public async Task<Asset?> GetAsset(long assetId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, CODE, DESCRIPTION, IS_MSI, CLIENT_ID FROM {_tableName} WHERE ID = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", assetId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localAsset = new Asset(reader.GetInt32(6))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MessageId = reader.GetString(2),
            Code = reader.GetString(3),
            Description = reader.GetString(4),
            IsMsi = reader.GetBoolean(5)
        };

        return localAsset;
    }

    public async Task DeleteAssetById(long assetId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE ID = {assetId}",
            connection);

        await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();
    }

    public async Task<int> GetAssetVersion(string clientId, long assetId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {assetId} AND CLIENT_ID = {clientId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.Read()) dbVersion = reader.GetInt32(0);

        ReturnConnection(connection);
        return dbVersion;
    }

    public async Task<int> GetAssetCountForClient(string clientId)
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

    public async Task<int> GetAssetCount()
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

    public Task BatchInsert(List<Asset> items)
    {
        throw new NotImplementedException();
    }

    public Task BatchUpdate(List<Asset> assets, int batchSize)
    {
        throw new NotImplementedException();
    }

    public Task BatchDelete(IEnumerable<long> ids)
    {
        throw new NotImplementedException();
    }

    public void Initialise()
    {
        var connection = GetConnection();

        var command = $"DROP TABLE IF EXISTS {_tableName}";
        var cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        command =
            $"CREATE TABLE {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, CODE TEXT, DESCRIPTION TEXT, IS_MSI INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        command = $"CREATE UNIQUE INDEX idx_{_tableName}_message_id ON {_tableName}(MESSAGE_ID)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        ReturnConnection(connection);
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
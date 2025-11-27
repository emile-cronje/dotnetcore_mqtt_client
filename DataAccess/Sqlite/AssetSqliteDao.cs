using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class AssetSqliteDao : SqliteDao, IAssetDao
{
    private readonly string _tableName;

    public AssetSqliteDao(IDbConnectionPool connectionPool) : base(connectionPool)
    {
        _tableName = "asset";
    }

    public async Task<int> AddAsset(string clientId, Asset asset)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (CLIENT_ID, REMOTE_ID, VERSION, CODE, DESCRIPTION, IS_MSI) VALUES ('{clientId}', '{asset.Id}', {asset.Version}, '{asset.Code}', '{asset.Description}', {asset.IsMsi})",
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
            $"UPDATE {_tableName} SET VERSION = {asset.Version}, CODE = '{asset.Code}', DESCRIPTION = '{asset.Description}', IS_MSI = {asset.IsMsi} WHERE REMOTE_ID = '{asset.Id}'",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);

        return rowsAffected;
    }

    public async Task<Asset?> GetAsset(long assetId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT remote_id, version, code, description, is_msi, client_id FROM {_tableName} WHERE remote_id = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", assetId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localAsset = new Asset(reader.GetInt32(5))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            Code = reader.GetString(2),
            Description = reader.GetString(3),
            IsMsi = reader.GetBoolean(4)
        };

        return localAsset;
    }

    public async Task DeleteAssetById(long assetId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE REMOTE_ID = '{assetId}'",
            connection);

        await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();
    }

    public async Task<int> GetAssetVersion(string clientId, long assetId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT version from {_tableName} where remote_id = '{assetId}' and client_id = '{clientId}'",
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
            new SqliteCommand($"SELECT count(*) from {_tableName} where client_id = '{clientId}'", connection);
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
            new SqliteCommand($"SELECT count(*) from {_tableName}", connection);
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
            $"CREATE TABLE {_tableName} (CLIENT_ID TEXT, REMOTE_ID TEXT, VERSION INTEGER, CODE TEXT, DESCRIPTION TEXT, IS_MSI INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        ReturnConnection(connection);
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
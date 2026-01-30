using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class MeterSqliteDao : SqliteDao, IMeterDao
{
    private readonly string _tableName;

    public MeterSqliteDao(IDbConnectionPool connectionPool, string tableName = "meter") : base(connectionPool)
    {
        _tableName = tableName;
    }

    public async Task<int> AddMeter(string clientId, Meter meter)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (ID, VERSION, CLIENT_ID, MESSAGE_ID, CODE, DESCRIPTION, IS_PAUSED) VALUES ({meter.Id}, {meter.Version}, {clientId}, '{meter.MessageId}', '{meter.Code}', '{meter.Description}', {meter.IsPaused})",
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

    public async Task<int> UpdateMeter(Meter meter)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = {meter.Version}, MESSAGE_ID = '{meter.MessageId}', CODE = '{meter.Code}', DESCRIPTION = '{meter.Description}', IS_PAUSED = {meter.IsPaused} WHERE ID = {meter.Id}",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<int> UpdateMeterOld(Meter meter)
    {
        var connection = GetConnection();

        // Fetch the current version from the database
        await using var versionCmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = @ID",
            connection);
        versionCmd.Parameters.AddWithValue("@ID", meter.Id);
        
        await using var versionReader = await versionCmd.ExecuteReaderAsync();
        if (!versionReader.Read())
        {
            ReturnConnection(connection);
            throw new InvalidOperationException($"Meter ID {meter.Id} not found");
        }
        
        var currentVersion = versionReader.GetInt32(0);

        // Now update using the current version from the database
        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = VERSION + 1, MESSAGE_ID = @MESSAGE_ID, CODE = @CODE, DESCRIPTION = @DESCRIPTION, IS_PAUSED = @IS_PAUSED WHERE ID = @ID AND VERSION = @VERSION",
            connection);
        cmd.Parameters.AddWithValue("@ID", meter.Id);
        cmd.Parameters.AddWithValue("@VERSION", currentVersion);
        cmd.Parameters.AddWithValue("@MESSAGE_ID", meter.MessageId);
        cmd.Parameters.AddWithValue("@CODE", meter.Code);
        cmd.Parameters.AddWithValue("@DESCRIPTION", meter.Description);
        cmd.Parameters.AddWithValue("@IS_PAUSED", meter.IsPaused);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);
        
        if (rowsAffected == 0)
        {
            throw new InvalidOperationException($"Version mismatch for Meter ID {meter.Id}. Current DB version {currentVersion}, in-memory version {meter.Version}");
        }

        return rowsAffected;
    }

    public async Task<Meter?> GetMeter(long meterId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, CODE, DESCRIPTION, IS_PAUSED, CLIENT_ID FROM {_tableName} WHERE ID = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", meterId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localMeter = new Meter(reader.GetInt32(6))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MessageId = reader.GetString(2),
            Code = reader.GetString(3),
            Description = reader.GetString(4),
            IsPaused = reader.GetBoolean(5)
        };

        return localMeter;
    }

    public async Task<int> GetMeterVersion(long meterId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {meterId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.Read()) dbVersion = reader.GetInt32(0);

        ReturnConnection(connection);
        return dbVersion;
    }

    public async Task<int> DeleteMeter(long meterId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE ID = {meterId}",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();

        return rowsAffected;
    }

    public async Task<int> GetMeterCountForClient(string clientId)
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

    public async Task<int> GetMeterCount()
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

    public void Initialise()
    {
        var connection = GetConnection();

        var command = $"DROP TABLE IF EXISTS {_tableName}";
        var cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        command =
            $"CREATE TABLE {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, CODE TEXT, DESCRIPTION TEXT, IS_PAUSED INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        command = $"CREATE UNIQUE INDEX idx_{_tableName}_message_id ON {_tableName}(MESSAGE_ID)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        InitializeWalMode(connection);

        ReturnConnection(connection);
    }

    public async Task<decimal?> GetMeterAdr(long meterId)
    {
        decimal? adr = 0;
        var connection = GetConnection();

        var sql = @"
SELECT AVG(COALESCE(daily_rate, 0)) AS average_daily_rate
FROM (
    SELECT
        (reading - LAG(reading) OVER (ORDER BY reading_on)) * 1.0
        /
        NULLIF(
            julianday(date(reading_on)) -
            julianday(date(LAG(reading_on) OVER (ORDER BY reading_on))),
            0
        ) AS daily_rate
    FROM meter_reading
    WHERE meter_id = @meterId
) AS daily_rates;        
                    ";

        await using (var cmd = new SqliteCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue("@meterId", meterId);

            var result = await cmd.ExecuteScalarAsync();

            if (result != DBNull.Value) adr = Convert.ToDecimal(result);
        }

        ReturnConnection(connection);
        return adr;
    }

    public async Task BatchInsert(List<Meter> meters)
    {
        foreach (var meter in meters) await AddMeter(meter.ClientId.ToString(), meter);
    }

    public async Task BatchUpdate(List<Meter> meters, int batchSize)
    {
        foreach (var meter in meters) await UpdateMeter(meter);
    }

    public async Task BatchDelete(IEnumerable<long> ids)
    {
        foreach (var id in ids) await DeleteMeter(id);
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